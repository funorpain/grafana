define([
  'angular',
  'lodash',
  'app/core/utils/datemath',
  'moment',
],
function (angular, _, dateMath) {
  'use strict';

  /** @ngInject */
  function OpenTsDatasource(instanceSettings, $q, backendSrv, templateSrv) {
    this.type = 'huya';
    this.url = instanceSettings.url;
    this.name = instanceSettings.name;
    this.withCredentials = instanceSettings.withCredentials;
    this.basicAuth = instanceSettings.basicAuth;
    instanceSettings.jsonData = instanceSettings.jsonData || {};
    this.tsdbVersion = instanceSettings.jsonData.tsdbVersion || 1;
    this.tsdbResolution = instanceSettings.jsonData.tsdbResolution || 1;
    this.supportMetrics = true;
    this.tagKeys = {};

    // Called once per panel (graph)
    this.query = function(options) {
      // console.log('options', options);
      var start = convertToTSDBTime(options.rangeRaw.from, false);
      var end = convertToTSDBTime(options.rangeRaw.to, true);
      var qs = [];
      var qsIndex = [];

      _.each(options.targets, function(target, index) {
        if (!target.metric) { return; }
        var query = convertTargetToQuery(target, options);
        if (query) {
          _.each(query, function(item) {
            var pos = -1;
            _.each(qs, function(val, i) {
              if (_.isEqual(val, item)) {
                pos = i;
                return false;
              }
            });
            if (pos === -1) {
              qs.push(item);
              qsIndex.push([index]);
            } else {
              qsIndex[pos].push(index);
            }
          });
        }
      });

      var queries = _.compact(qs);

      // No valid targets, return the empty result to save a round trip.
      if (_.isEmpty(queries)) {
        var d = $q.defer();
        d.resolve({ data: [] });
        return d.promise;
      }

      var groupByTags = {};
      _.each(queries, function(query) {
        if (query.filters && query.filters.length > 0) {
          _.each(query.filters, function(val) {
            groupByTags[val.tagk] = true;
          });
        } else {
          _.each(query.tags, function(val, key) {
            groupByTags[key] = true;
          });
        }
      });

      // console.log('queries', queries);
      // console.log('qsIndex', qsIndex);
      return this.performTimeSeriesQuery(queries, start, end).then(function(response) {
        var metricToTargetMapping = mapMetricsToTargets(response.data, options, this.tsdbVersion);
        // console.log('response.data', response.data);
        // console.log('metricToTargetMapping', metricToTargetMapping);
        var result = [];
        _.each(response.data, function(metricData, index) {
          var indexes = qsIndex[metricToTargetMapping[index]];
          _.each(indexes, function(index) {
            var target = options.targets[index],
                metric = templateSrv.replace(target.metric, options.scopedVars, 'pipe'),
                avg = metric.endsWith('_avg');

            if (!avg) {
              this._saveTagKeys(metricData);
              result.push(transformMetricData(metricData, groupByTags, target, options, this.tsdbResolution));
              return;
            }

            if (!metricData.metric.endsWith('_cnt')) {
              return;
            }
            var avgData = _.clone(metricData);
            avgData.metric = avgData.metric.slice(0, -4) + '_avg';
            this._saveTagKeys(avgData);

            var sumList = [];
            _.each(response.data, function(refData, refIndex) {
              if (refData.metric.endsWith('_sum') && qsIndex[metricToTargetMapping[refIndex]].indexOf(index) !== -1
                  && this.isSubsetOfTags(avgData.tags, refData.tags)) {
                sumList.push(refData);
              }
            }.bind(this));
            if (sumList.length === 0) {
              return;
            }
            processMetricData(avgData, sumList, target, options);

            var hasData = false;
            _.each(avgData.dps, function(value) {
              if (value !== null) {
                hasData = true;
                return false;
              }
            });
            if (!hasData) {
              return;
            }

            result.push(transformMetricData(avgData, groupByTags, target, options, this.tsdbResolution));
          }.bind(this));
        }.bind(this));
        // console.log('result', result);
        return { data: result };
      }.bind(this));
    };

    this.isSubsetOfTags = function(a, b) {
      for (var k in a) {
        if (!(k in b)) {
          return false;
        }
      }
      return true;
    };

    this.annotationQuery = function(options) {
      var start = convertToTSDBTime(options.rangeRaw.from, false);
      var end = convertToTSDBTime(options.rangeRaw.to, true);
      var qs = [];
      var eventList = [];

      qs.push({ aggregator:"sum", metric:options.annotation.target });

      var queries = _.compact(qs);

      return this.performTimeSeriesQuery(queries, start, end).then(function(results) {
        if(results.data[0]) {
          var annotationObject = results.data[0].annotations;
          if(options.annotation.isGlobal){
            annotationObject = results.data[0].globalAnnotations;
          }
          if(annotationObject) {
            _.each(annotationObject, function(annotation) {
              var event = {
                title: annotation.description,
                time: Math.floor(annotation.startTime) * 1000,
                text: annotation.notes,
                annotation: options.annotation
              };

              eventList.push(event);
            });
          }
        }
        return eventList;

      }.bind(this));
    };

    this.targetContainsTemplate = function(target) {
      if (target.filters && target.filters.length > 0) {
        for (var i = 0; i < target.filters.length; i++) {
          if (templateSrv.variableExists(target.filters[i].filter)) {
            return true;
          }
        }
      }

      if (target.tags && Object.keys(target.tags).length > 0) {
        for (var tagKey in target.tags) {
          if (templateSrv.variableExists(target.tags[tagKey])) {
            return true;
          }
        }
      }

      return false;
    };

    this.performTimeSeriesQuery = function(queries, start, end) {
      var msResolution = false;
      if (this.tsdbResolution === 2) {
        msResolution = true;
      }
      var reqBody = {
        start: start,
        queries: queries,
        msResolution: msResolution,
        globalAnnotations: true
      };
      if (this.tsdbVersion === 3) {
        reqBody.showQuery = true;
      }

      // Relative queries (e.g. last hour) don't include an end time
      if (end) {
        reqBody.end = end;
      }

      var options = {
        method: 'POST',
        url: this.url + '/api/query',
        data: reqBody
      };

      this._addCredentialOptions(options);
      return backendSrv.datasourceRequest(options);
    };

    this.suggestTagKeys = function(metric) {
      return $q.when(this.tagKeys[metric] || []);
    };

    this._saveTagKeys = function(metricData) {
      var tagKeys = Object.keys(metricData.tags);
      _.each(metricData.aggregateTags, function(tag) {
        tagKeys.push(tag);
      });

      this.tagKeys[metricData.metric] = tagKeys;
    };

    this._performSuggestQuery = function(query, type) {
      return this._get('/api/suggest', {type: type, q: query, max: 1000}).then(function(result) {
        return result.data;
      });
    };

    this._performMetricKeyValueLookup = function(metric, keys) {

      if(!metric || !keys) {
        return $q.when([]);
      }

      var keysArray = keys.split(",").map(function(key) {
        return key.trim();
      });
      var key = keysArray[0];
      var keysQuery = key + "=*";

      if (keysArray.length > 1) {
        keysQuery += "," + keysArray.splice(1).join(",");
      }

      var m = metric + "{" + keysQuery + "}";

      return this._get('/api/search/lookup', {m: m, limit: 3000}).then(function(result) {
        result = result.data.results;
        var tagvs = [];
        _.each(result, function(r) {
          if (tagvs.indexOf(r.tags[key]) === -1) {
            tagvs.push(r.tags[key]);
          }
        });
        return tagvs;
      });
    };

    this._performMetricKeyLookup = function(metric) {
      if(!metric) { return $q.when([]); }

      return this._get('/api/search/lookup', {m: metric, limit: 1000}).then(function(result) {
        result = result.data.results;
        var tagks = [];
        _.each(result, function(r) {
          _.each(r.tags, function(tagv, tagk) {
            if(tagks.indexOf(tagk) === -1) {
              tagks.push(tagk);
            }
          });
        });
        return tagks;
      });
    };

    this._performThresholdMetricKeyValueLookup = function(threshold, tag, start, end, m) {
      if(!tag || !start || !m) {
        return $q.when([]);
      }

      return this._get('/api/query', {start: start, end: end, m: m}).then(function(result) {
        var tagvs = [];
        _.each(result.data, function(r) {
          _.each(r.dps, function(value) {
            if (value >= threshold) {
              if (tagvs.indexOf(r.tags[tag]) === -1) {
                tagvs.push({
                  'text': r.tags[tag] + ' (' + value + ')',
                  'value': r.tags[tag]
                });
              }
              return false;
            }
          });
        });
        return tagvs;
      });
    };

    this._get = function(relativeUrl, params) {
      var options = {
        method: 'GET',
        url: this.url + relativeUrl,
        params: params,
      };

      this._addCredentialOptions(options);

      return backendSrv.datasourceRequest(options);
    };

    this._performGetJson = function(url) {
      return backendSrv.datasourceRequest({
        url: url,
        method: 'GET',
        headers: { 'Content-Type': 'application/json' }
      }).then(function(result) {
        if (result.data && _.isObject(result.data.data) && !_.isArray(result.data.data)) {
          return _.map(result.data.data, function(value, key) {
            return {
              'text': value,
              'value': key
            };
          });
        }
        return [];
      });
    };

    this._addCredentialOptions = function(options) {
      if (this.basicAuth || this.withCredentials) {
        options.withCredentials = true;
      }
      if (this.basicAuth) {
        options.headers = {"Authorization": this.basicAuth};
      }
    };

    this.metricFindQuery = function(query) {
      if (!query) { return $q.when([]); }

      var interpolated;
      try {
        interpolated = templateSrv.replace(query, {}, 'distributed');
      }
      catch (err) {
        return $q.reject(err);
      }

      var responseTransform = function(result) {
        return _.map(result, function(value) {
          return {text: value};
        });
      };

      var metrics_regex = /metrics\((.*)\)/;
      var tag_names_regex = /tag_names\((.*)\)/;
      var tag_values_regex = /tag_values\((.*?),\s?(.*)\)/;
      var threshold_tag_values_regex = /tag_threshold_values\((.*?),\s?(.*?),\s?(.*?),\s?(.*?),\s?(.*)\)/;
      var tag_names_suggest_regex = /suggest_tagk\((.*)\)/;
      var tag_values_suggest_regex = /suggest_tagv\((.*)\)/;
      var getjson_regex = /getjson\((.*)\)/;

      var metrics_query = interpolated.match(metrics_regex);
      if (metrics_query) {
        return this._performSuggestQuery(metrics_query[1], 'metrics').then(responseTransform);
      }

      var tag_names_query = interpolated.match(tag_names_regex);
      if (tag_names_query) {
        return this._performMetricKeyLookup(tag_names_query[1]).then(responseTransform);
      }

      var tag_values_query = interpolated.match(tag_values_regex);
      if (tag_values_query) {
        return this._performMetricKeyValueLookup(tag_values_query[1], tag_values_query[2]).then(responseTransform);
      }

      var threshold_tag_values_query = interpolated.match(threshold_tag_values_regex);
      if (threshold_tag_values_query) {
        return this._performThresholdMetricKeyValueLookup(threshold_tag_values_query[1], threshold_tag_values_query[2],
          threshold_tag_values_query[3], threshold_tag_values_query[4], threshold_tag_values_query[5]);
      }

      var tag_names_suggest_query = interpolated.match(tag_names_suggest_regex);
      if (tag_names_suggest_query) {
        return this._performSuggestQuery(tag_names_suggest_query[1], 'tagk').then(responseTransform);
      }

      var tag_values_suggest_query = interpolated.match(tag_values_suggest_regex);
      if (tag_values_suggest_query) {
        return this._performSuggestQuery(tag_values_suggest_query[1], 'tagv').then(responseTransform);
      }

      var getjson_query = interpolated.match(getjson_regex);
      if (getjson_query) {
        return this._performGetJson(getjson_query[1]);
      }

      return $q.when([]);
    };

    this.testDatasource = function() {
      return this._performSuggestQuery('cpu', 'metrics').then(function () {
        return { status: "success", message: "Data source is working", title: "Success" };
      });
    };

    var aggregatorsPromise = null;
    this.getAggregators = function() {
      if (aggregatorsPromise) { return aggregatorsPromise; }

      aggregatorsPromise = this._get('/api/aggregators').then(function(result) {
        if (result.data && _.isArray(result.data)) {
          return result.data.sort();
        }
        return [];
      });
      return aggregatorsPromise;
    };

    var filterTypesPromise = null;
    this.getFilterTypes = function() {
      if (filterTypesPromise) { return filterTypesPromise; }

      filterTypesPromise = this._get('/api/config/filters').then(function(result) {
        if (result.data) {
          return Object.keys(result.data).sort();
        }
        return [];
      });
      return filterTypesPromise;
    };

    function processMetricData(metricData, refData, target, options) {
      var dps = {};
      var threshold = target.threshold ? templateSrv.replace(target.threshold, options.scopedVars, 'pipe') : 0;

      _.each(metricData.dps, function(value, key) {
        if (value >= threshold && value > 0) {
          var sum = 0;
          for (var i = 0, n = refData.length; i < n; i++) {
            var v = refData[i].dps[key];
            if (v) {
              sum += v;
            }
          }
          dps[key] = sum / value;
        } else {
          dps[key] = null;
        }
      });

      metricData.dps = dps;
    }

    function transformMetricData(md, groupByTags, target, options, tsdbResolution) {
      var metricLabel = createMetricLabel(md, target, groupByTags, options);
      var dps = [];

      // TSDB returns datapoints has a hash of ts => value.
      // Can't use _.pairs(invert()) because it stringifies keys/values
      _.each(md.dps, function (v, k) {
        if (tsdbResolution === 2) {
          dps.push([v, k * 1]);
        } else {
          dps.push([v, k * 1000]);
        }
      });

      return { target: metricLabel, datapoints: dps };
    }

    function createMetricLabel(md, target, groupByTags, options) {
      if (target.alias) {
        var scopedVars = _.clone(options.scopedVars || {});
        _.each(md.tags, function(value, key) {
          scopedVars['tag_' + key] = {value: value};
        });
        return templateSrv.replace(target.alias, scopedVars);
      }

      var label = md.metric;
      var tagData = [];

      if (!_.isEmpty(md.tags)) {
        _.each(_.toPairs(md.tags), function(tag) {
          if (_.has(groupByTags, tag[0])) {
            tagData.push(tag[0] + "=" + tag[1]);
          }
        });
      }

      if (!_.isEmpty(tagData)) {
        label += "{" + tagData.join(", ") + "}";
      }

      return label;
    }

    function convertTargetToQuery(target, options) {
      if (!target.metric || target.hide) {
        return null;
      }

      var query = {
        metric: templateSrv.replace(target.metric, options.scopedVars, 'pipe'),
        aggregator: "sum"
      };

      if (!query.metric.match(/_(sum|cnt|avg)$/)) {
        return null;
      }

      var avg = query.metric.endsWith('_avg');

      if (target.filters && target.filters.length > 0) {
        query.filters = angular.copy(target.filters);
        if (query.filters){
          for (var filter_key in query.filters) {
            query.filters[filter_key].filter = templateSrv.replace(query.filters[filter_key].filter, options.scopedVars, 'pipe');
          }
        }
      } else {
        query.tags = angular.copy(target.tags);
        if (query.tags){
          for (var tag_key in query.tags) {
            query.tags[tag_key] = templateSrv.replace(query.tags[tag_key], options.scopedVars, 'pipe');
          }
        }
      }

      if (target.explicitTags) {
        query.explicitTags = true;
      }

      if (!target.disableDownsampling) {
        var interval =  templateSrv.replace(target.downsampleInterval || options.interval);

        if (interval.match(/\.[0-9]+s/)) {
          interval = parseFloat(interval)*1000 + "ms";
        }

        query.downsample = interval + "-" + "sum";
      }

      if (!avg) {
        return [query];
      }

      query.metric = query.metric.slice(0, -4) + '_sum';

      var cntQuery = angular.copy(query);
      cntQuery.metric = cntQuery.metric.slice(0, -4) + '_cnt';

      return [query, cntQuery];
    }

    function mapMetricsToTargets(metrics, options, tsdbVersion) {
      var interpolatedTagValue;
      return _.map(metrics, function(metricData) {
        if (tsdbVersion === 3) {
          return metricData.query.index;
        } else {
          return _.findIndex(options.targets, function(target) {
            if (target.filters && target.filters.length > 0) {
              return target.metric === metricData.metric;
            } else {
              return target.metric === metricData.metric &&
              _.every(target.tags, function(tagV, tagK) {
                interpolatedTagValue = templateSrv.replace(tagV, options.scopedVars, 'pipe');
                return metricData.tags[tagK] === interpolatedTagValue || interpolatedTagValue === "*";
              });
            }
          });
        }
      });
    }

    function convertToTSDBTime(date, roundUp) {
      if (date === 'now') {
        return null;
      }

      date = dateMath.parse(date, roundUp);
      return date.valueOf();
    }
  }

  return {
    OpenTsDatasource: OpenTsDatasource
  };
});
