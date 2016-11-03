/*jshint node:true, laxcomma:true */

var util    = require('util')
  , config = require('./lib/config')
  , helpers = require('./lib/helpers')
  , fs     = require('fs')
  , events = require('events')
  , logger = require('./lib/logger')
  , set = require('./lib/set')
  , pm = require('./lib/process_metrics')
  , process_mgmt = require('./lib/process_mgmt')
  , mgmt_server = require('./lib/mgmt_server')
  , mgmt = require('./lib/mgmt_console');


// initialize data structures with defaults for statsd stats
var keyCounter = {};
var counter_rates = {};
var timer_data = {};
var pctThreshold = null;
var flushInterval, keyFlushInt, serversLoaded, mgmtServer;
var startup_time = Math.round(new Date().getTime() / 1000);
var backendEvents = new events.EventEmitter();
var healthStatus = config.healthStatus || 'up';
var timestamp_lag_namespace;
var keyNameSanitize = true;

// Load and init the backend from the backends/ directory.
function loadBackend(config, name) {
  var backendmod = require(name);

  if (config.debug) {
    l.log("Loading backend: " + name, 'DEBUG');
  }

  var ret = backendmod.init(startup_time, config, backendEvents, l);
  if (!ret) {
    l.log("Failed to load backend: " + name, "ERROR");
    process.exit(1);
  }
}

// Load and init the server from the servers/ directory.
// The callback mimics the dgram 'message' event parameters (msg, rinfo)
//   msg: the message received by the server. may contain more than one metric
//   rinfo: contains remote address information and message length
//      (attributes are .address, .port, .family, .size - you're welcome)
function startServer(config, name, callback) {
  var servermod = require(name);

  if (config.debug) {
    l.log("Loading server: " + name, 'DEBUG');
  }

  var ret = servermod.start(config, callback);
  if (!ret) {
    l.log("Failed to load server: " + name, "ERROR");
    process.exit(1);
  }
}

// global for conf
var conf;

function TimeslotStats(default_metric_keys, conf) {
  this.default_metric_keys = default_metric_keys;
  this.slots = [];
  this.conf = conf;
  this.latest_update = new Date().getTime();
};
TimeslotStats.prototype.getStatsIndex = function(ts, interval) {
  if (!interval) {
    interval = conf.flushInterval;
  }
  return Math.ceil(ts / interval) * interval;
};
TimeslotStats.prototype.getCompleted = function(ts, interval) {
  var latest_completed = Math.floor(ts / interval) * interval;
  var out = [];
  for (var i = 0; i < this.slots.length; i++) {
    if (this.slots[i].time_stamp <= latest_completed && !this.slots[i].sent) {
      this.slots[i].sent = true;
      out.push(this.slots[i]);
    }
  }
  this.slots = this.slots.filter(function (elm) {
    return !elm.sent;
  });
  return out;
};
TimeslotStats.prototype.ensureSlot = function(ts, interval) {
  while((ts - this.latest_update) >= interval) {
    this.getSlot(this.getStatsIndex(ts, interval));
    ts -= interval;
  }
};
TimeslotStats.prototype.getSlot = function(ts_index) {
  for (var i = this.slots.length - 1; i >= 0; i--) {
    if (this.slots[i].time_stamp === ts_index) {
      return this.slots[i];
    }
  }
  var out = this.newSlot(ts_index);
  this.slots.push(out);
  this.latest_update = ts_index;
  return out;
};
TimeslotStats.prototype.newSlot = function(ts_index) {
  var last = null;
  if (this.slots.length > 0) {
    last = this.slots[this.slots.length - 1];
  }

  var prefill = function(obj, arr) {
    if (arr) {
      for(var i in arr) {
        obj[arr[i]] = 0;
      }
    }
  }

  // Copy counters if needed
  var deleteCounters = this.conf.deleteCounters || false;
  var counters = {};
  prefill(counters, this.default_metric_keys.counters);
  if (!deleteCounters && !!last) {
    for (var i in last.counters) {
      counters[i] = 0;
    }
  }

  // Copy timers if needed
  var deleteTimers = this.conf.deleteTimers || false;
  var timers = {};
  var timer_counters = {};
  if (!deleteTimers && !!last) {
    for (var i in last.timers) {
      timers[i] = [];
      timer_counters[i] = 0;
    }
  }

  //  Copy sets if needed
  var deleteSets = this.conf.deleteSets || false;
  var sets = {};
  if (!deleteSets && !!last) {
    for (var i in last.sets) {
      sets[i] = new set.Set();
    }
  }

  // Normally gauges are not reset.  so if we don't delete them, continue to persist previous value
  var deleteGauges = this.conf.deleteGauges || false;
  var gauges = {};
  //prefill(gauges, this.default_metric_keys.gauges);
  if (!deleteGauges && !!last) {
    for (var i in last.gauges) {
      gauges[i] = last.gauges[i];  // assuming this is an int, just copy
    }
  }

  return {
    time_stamp: ts_index,
    counters: counters,
    gauges: gauges,
    timers: timers,
    timer_counters: timer_counters,
    sets: sets,
  };
};

var stats_holder = null;

// Flush metrics to each backend.
first_send = true;
function flushMetrics() {
  var ts = new Date().getTime();
  // ensure slot for the tick
  stats_holder.ensureSlot(ts, conf.flushInterval);
  var to_flush = stats_holder.getCompleted(ts, conf.flushInterval);
  for (var i = 0; i < to_flush.length; i++) {
    var slot = to_flush[i];
    var time_stamp = slot.time_stamp;
    var time_stamp_secs = Math.round(time_stamp / 1000);
    if (first_send) {
      // TODO: remove first_send non sense by fixing the tests:
      // most of them check that the numStats of the first packet does not contain timestamp_lag..
      first_send = false;
    } else {
      slot.gauges[timestamp_lag_namespace] = (ts - time_stamp);
      console.log('||SENDING ', timestamp_lag_namespace, ': ', slot.gauges[timestamp_lag_namespace], ' -- ', ts, ' - ', time_stamp);
    }
    var metrics_hash = {
      counters: slot.counters,
      gauges: slot.gauges,
      timers: slot.timers,
      timer_counters: slot.timer_counters,
      sets: slot.sets,
      counter_rates: counter_rates,
      timer_data: timer_data,
      pctThreshold: pctThreshold,
      histogram: conf.histogram
    };
    pm.process_metrics(metrics_hash, conf.flushInterval, time_stamp_secs, function emitFlush(metrics) {
      backendEvents.emit('flush', time_stamp_secs, metrics);
    });
  }
}

var stats = {
  messages: {
    last_msg_seen: startup_time,
    bad_lines_seen: 0
  }
};

function sanitizeKeyName(key) {
  if (keyNameSanitize) {
    return key.replace(/\s+/g, '_')
              .replace(/\//g, '-')
              .replace(/[^a-zA-Z_\-0-9\.]/g, '');
  } else {
    return key;
  }
}

// Global for the logger
var l;

config.configFile(process.argv[2], function (config) {
  conf = config;

  process_mgmt.init(config);

  l = new logger.Logger(config.log || {});

  // setup config for stats prefix
  var prefixStats = config.prefixStats;
  prefixStats = prefixStats !== undefined ? prefixStats : "statsd";
  //setup the names for the stats stored in counters{}
  bad_lines_seen   = prefixStats + ".bad_lines_seen";
  packets_received = prefixStats + ".packets_received";
  metrics_received = prefixStats + ".metrics_received";
  timestamp_lag_namespace = prefixStats + ".timestamp_lag";

  stats_holder = new TimeslotStats(
    {counters: [bad_lines_seen, packets_received, metrics_received],
     //gauges: [timestamp_lag_namespace]
    },
    conf);
  // initialize stats
  stats_holder.ensureSlot(new Date().getTime(), config.flushInterval);

  var getSlot = function(idx) {
    return stats_holder.getSlot(idx);
  };
  var ensureStats = function(stat, name, def) {
    if (stat[name] === undefined) {
      stat[name] = def;
    }
  };

  var incCounter = function(idx, name, step) {
    var slot = getSlot(idx);
    ensureStats(slot.counters, name, 0);
    step = step || 1;
    slot.counters[name] += step;
  };
  var updateTimer = function(idx, name, timer_data, timer_counter_data) {
    var slot = getSlot(idx);
    ensureStats(slot.timers, name, []);
    ensureStats(slot.timer_counters, name, 0);
    slot.timers[name].push(timer_data);
    slot.timer_counters[name] += timer_counter_data;
  };
  var updateOrOverrideGauge = function(idx, name, update, gauge_data) {
    var slot = getSlot(idx);
    ensureStats(slot.gauges, name, 0);
    if (update) {
      slot.gauges[name] += gauge_data;
    } else {
      slot.gauges[name] = gauge_data;
    }
  };
  var addToSet = function(idx, name, elm) {
    var slot = getSlot(idx);
    if (!slot.sets[name]) {
      slot.sets[name] = new set.Set();
    }
    slot.sets[name].insert(elm);
  };


  conf.deleteIdleStats = conf.deleteIdleStats !== undefined ? conf.deleteIdleStats : false;
  if (conf.deleteIdleStats) {
    conf.deleteCounters = conf.deleteCounters !== undefined ? conf.deleteCounters : true;
    conf.deleteTimers = conf.deleteTimers !== undefined ? conf.deleteTimers : true;
    conf.deleteSets = conf.deleteSets !== undefined ? conf.deleteSets : true;
    conf.deleteGauges = conf.deleteGauges !== undefined ? conf.deleteGauges : true;
  }

  if (config.keyNameSanitize !== undefined) {
    keyNameSanitize = config.keyNameSanitize;
  }
  if (!serversLoaded) {

    // key counting
    var keyFlushInterval = Number((config.keyFlush && config.keyFlush.interval) || 0);

    var handlePacket = function (msg, rinfo) {
      var ts = new Date().getTime();
      var stats_index = stats_holder.getStatsIndex(ts, config.flushInterval);
      backendEvents.emit('packet', msg, rinfo);
      incCounter(stats_index, packets_received);
      var metrics;
      var packet_data = msg.toString();
      if (packet_data.indexOf("\n") > -1) {
        metrics = packet_data.split("\n");
      } else {
        metrics = [ packet_data ] ;
      }
      console.log('I>I>I>METRICS ', metrics);

      for (var midx in metrics) {
        if (metrics[midx].length === 0) {
          continue;
        }

        incCounter(stats_index, metrics_received);
        if (config.dumpMessages) {
          l.log(metrics[midx].toString());
        }
        var bits = metrics[midx].toString().split(':');
        var key = sanitizeKeyName(bits.shift());

        if (keyFlushInterval > 0) {
          if (! keyCounter[key]) {
            keyCounter[key] = 0;
          }
          keyCounter[key] += 1;
        }

        if (bits.length === 0) {
          bits.push("1");
        }

        for (var i = 0; i < bits.length; i++) {
          var sampleRate = 1;
          var fields = bits[i].split("|");
          if (!helpers.is_valid_packet(fields)) {
              l.log('Bad line: ' + fields + ' in msg "' + metrics[midx] +'"');
              incCounter(stats_index, bad_lines_seen);
              stats.messages.bad_lines_seen++;
              continue;
          }
          if (fields[2]) {
            sampleRate = Number(fields[2].match(/^@([\d\.]+)/)[1]);
          }

          var metric_type = fields[1].trim();
          if (metric_type === "ms") {
            console.log('I>I>I> UPDATE TIMER ', fields);
            updateTimer(stats_index, key, Number(fields[0] || 0), (1 / sampleRate));
            console.log('I>I>I> UPDATED TIMERS ', getSlot(stats_index).timers);
          } else if (metric_type === "g") {
            updateOrOverrideGauge(stats_index, key, fields[0].match(/^[-+]/), Number(fields[0] || 0));
            // if (getGauges(stats_index)[key] && fields[0].match(/^[-+]/)) {
            //   getGauges(stats_index)[key] += Number(fields[0] || 0);
            // } else {
            //   getGauges(stats_index)[key] = Number(fields[0] || 0);
            // }
          } else if (metric_type === "s") {
            addToSet(stats_index, key, fields[0] || '0');
          } else {
            incCounter(stats_index, key, Number(fields[0] || 1) * (1 / sampleRate));
          }
        }
      }

      stats.messages.last_msg_seen = Math.round(new Date().getTime() / 1000);
    };

    // If config.servers isn't specified, use the top-level config for backwards-compatibility
    var server_config = config.servers || [config];
    for (var i = 0; i < server_config.length; i++) {
      // The default server is UDP
      var server = server_config[i].server || './servers/udp';
      startServer(server_config[i], server, handlePacket);
    }

    mgmt_server.start(
      config,
      function(cmd, parameters, stream) {
        var stats_index = stats_holder.getStatsIndex(new Date().getTime(), config.flushInterval);
        var slot = getSlot(stats_index);
        switch(cmd) {
          case "help":
            stream.write("Commands: stats, counters, timers, gauges, delcounters, deltimers, delgauges, health, config, quit\n\n");
            break;

          case "config":
            helpers.writeConfig(config, stream);
            break;

          case "health":
            if (parameters.length > 0) {
              var cmdaction = parameters[0].toLowerCase();
              if (cmdaction === 'up') {
                healthStatus = 'up';
              } else if (cmdaction === 'down') {
                healthStatus = 'down';
              }
            }
            stream.write("health: " + healthStatus + "\n");
            break;

          case "stats":
            var now    = Math.round(new Date().getTime() / 1000);
            var uptime = now - startup_time;

            stream.write("uptime: " + uptime + "\n");

            var stat_writer = function(group, metric, val) {
              var delta;

              if (metric.match("^last_")) {
                delta = now - val;
              }
              else {
                delta = val;
              }

              stream.write(group + "." + metric + ": " + delta + "\n");
            };

            // Loop through the base stats
            for (var group in stats) {
              for (var metric in stats[group]) {
                stat_writer(group, metric, stats[group][metric]);
              }
            }

            backendEvents.once('status', function(writeCb) {
              stream.write("END\n\n");
            });

            // Let each backend contribute its status
            backendEvents.emit('status', function(err, name, stat, val) {
              if (err) {
                l.log("Failed to read stats for backend " +
                        name + ": " + err);
              } else {
                stat_writer(name, stat, val);
              }
            });

            break;

          case "counters":
            stream.write(util.inspect(slot.counters) + "\n");
            stream.write("END\n\n");
            break;

          case "timers":
            stream.write(util.inspect(slot.timers) + "\n");
            stream.write("END\n\n");
            break;

          case "gauges":
            stream.write(util.inspect(slot.gauges) + "\n");
            stream.write("END\n\n");
            break;

          case "delcounters":
            mgmt.delete_stats(slot.counters, parameters, stream);
            break;

          case "deltimers":
            mgmt.delete_stats(slot.timers, parameters, stream);
            break;

          case "delgauges":
            mgmt.delete_stats(slot.gauges, parameters, stream);
            break;

          case "curtimeslot":
            stream.write(util.inspect(slot) + "\n");
            stream.write("END\n\n");
            break;

          case "timeslots":
            stream.write(util.inspect(stats_holder.slots) + "\n");
            stream.write("END\n\n");
            break;

          case "quit":
            stream.end();
            break;

          default:
            stream.write("ERROR\n");
            break;
        }
      },
      function(err, stream) {
        l.log('MGMT: Caught ' + err +', Moving on', 'WARNING');
      }
    );

    serversLoaded = true;
    util.log("server is up", "INFO");

    pctThreshold = config.percentThreshold || 90;
    if (!Array.isArray(pctThreshold)) {
      pctThreshold = [ pctThreshold ]; // listify percentiles so single values work the same
    }

    flushInterval = Number(config.flushInterval || 10000);
    config.flushInterval = flushInterval;

    if (config.backends) {
      for (var j = 0; j < config.backends.length; j++) {
        loadBackend(config, config.backends[j]);
      }
    } else {
      // The default backend is graphite
      loadBackend(config, './backends/graphite');
    }

    // Setup the flush interval - should be safe to increase the calling interval
    var flushInt = setInterval(flushMetrics, flushInterval / (config.flushRate || 2));

    if (keyFlushInterval > 0) {
      var keyFlushPercent = Number((config.keyFlush && config.keyFlush.percent) || 100);
      var keyFlushLog = config.keyFlush && config.keyFlush.log;

      keyFlushInt = setInterval(function () {
        var sortedKeys = [];

        for (var key in keyCounter) {
          sortedKeys.push([key, keyCounter[key]]);
        }

        sortedKeys.sort(function(a, b) { return b[1] - a[1]; });

        var logMessage = "";
        var timeString = (new Date()) + "";

        // only show the top "keyFlushPercent" keys
        for (var i = 0, e = sortedKeys.length * (keyFlushPercent / 100); i < e; i++) {
          logMessage += timeString + " count=" + sortedKeys[i][1] + " key=" + sortedKeys[i][0] + "\n";
        }

        if (keyFlushLog) {
          var logFile = fs.createWriteStream(keyFlushLog, {flags: 'a+'});
          logFile.write(logMessage);
          logFile.end();
        } else {
          process.stdout.write(logMessage);
        }

        // clear the counter
        keyCounter = {};
      }, keyFlushInterval);
    }
  }
});

process.on('exit', function () {
  flushMetrics();
});
