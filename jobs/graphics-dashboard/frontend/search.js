"use strict";
var Search = {};

Search.DoesDeviceMatch = function (device, search)
{
  for (var i = 0; i < search.length; i++) {
    if (device == search[i])
      return true;
  }
  return false;
}

Search.ByDevices = function (devices, search)
{
  var total = 0;
  var found = 0;
  for (var key in devices) {
    total += devices[key];
    if (Search.DoesDeviceMatch(key, search))
      found += devices[key];
  }
  return [found, total];
}

Search.DoesTermMatch = function (item, terms)
{
  for (var i = 0; i < terms.length; i++) {
    var term = terms[i];
    if (typeof(term) == 'string') {
      if (item == term)
        return true;
    } else {
      if (term.test(item))
        return true;
    }
  }
  return false;
}

Search.ByTerm = function (items, search)
{
  var total = 0;
  var found = 0;
  for (var key in items) {
    total += items[key];
    if (Search.DoesTermMatch(key, search))
      found += items[key];
  }
  return [found, total];
}
