$("#intersectForm").bind('submit', function(event) {
    event.preventDefault();
    var query = makeIntersectQuery();
    if (!query) {
        console.log("No query.");
        return
    }
    console.log("Q", query);
    doAjax('query', query, function(data) {
        if (data.error) {
            console.log("QError", data.error);
            $('#intersectResults').hide();
        } else {
            $('#intersectResults').show();
        }
        $("#intersect-result-query").html(query);
        $("#intersect-result-latency").text(data['seconds'].toString().substring(0,5) + ' sec');
        $("#intersect-result-count").text(addCommas(data['rows'][0].count) + ' rides');
        $("#intersect-result-total").text(addCommas(data['numProfiles']) + ' total rides');
    });
});

$("#topn").bind('submit', function(event) {
    event.preventDefault();
    $('#topnResults').hide()
    var data = $('#topNForm').serialize()
    doAjax('query/topn', data, function(data) {
        console.log(data);
        if (data.error) {
            $('#topnResults').hide()
        } else {
            $('#topnResults').show()
        }
        $("#topn-result-query").text(data['query']);
        $("#topn-result-latency").text(data['seconds'].toString().substring(0,5) + ' sec');
        $("#topn-result-total").text(addCommas(data['numProfiles']) + ' total rides');

        var table = $('<table class="table"><thead><tr><th>Bitmap ID</th><th>Count</th></tr></thead></table>');
        var tbody = $('<tbody></tbody>');
        $.each(data['rows'], function(index, obj) {
            tbody.append('<tr><td>' + obj.bitmapID + '</td><td>' + obj.count + '</td></tr>')
        });
        table.append(tbody);
        $("#topn-result-table").html(table);

        clearPlot("#topn-plot-container");
        if(data['rows'].length > 1 && 'count' in data['rows'][0]) {
            renderHistogram(data['rows'], "#topn-plot-container");
        }

    });
});

$("#p1").click(function(event) {
    predefined('predefined/1', "");
});

$("#p2").click(function(event) {
    predefined('predefined/2', "");
});

$("#p3").click(function(event) {
    predefined('predefined/3', "");
});

$("#p4").click(function(event) {
    predefined('predefined/4', "");
});

$("#p5").click(function(event) {
    predefined('predefined/5', "");
});

function predefined(url, data) {
    $('#predefinedResults').hide()
    return doAjax(url, data, function(data) {
        console.log(data);
        if (data.error) {
            $('#predefinedResults').hide()
        } else {
            $('#predefinedResults').show()
        }
        $("#predefined-result-description").text(data['description']);
        $("#predefined-result-latency").text(data['seconds'].toString().substring(0,5) + ' sec');
        $("#predefined-result-total").text(addCommas(data['numProfiles']) + ' total rides');

        var header = [];
        var table = $('<table class="table"></table>');
        var theadtr = $('<tr></tr>')
        $.each(data['rows'][0], function(key) {
            header.push(key);
            theadtr.append($('<th>' + key + '</th>'));
        });
        var thead = $('<thead></thead>');
        thead.append(theadtr);
        table.append(thead);
        var tbody = $('<tbody></tbody>');
        $.each(data['rows'], function(index, row) {
            var tr = $('<tr></tr>');
            $.each(header, function(_, key) {
                tr.append($('<td>' + row[key] + '</td>'));
            });
            tbody.append(tr);
        });
        table.append(tbody);
        $("#predefined-result-table").html(table);

        clearPlot("#predefined-plot-container");        
        if(data['rows'].length > 1 && 'count' in data['rows'][0]) {
            renderHistogram(data['rows'], "#predefined-plot-container");
        }
    });
}

function doAjax(url, data, callback) {
    $.ajax({
        url: url,
        type: 'get',
        dataType: 'json',
        data: data,
        success: callback,
    });
}

function renderResultsAscii(rows) {
    str = ""
    for(var key in rows[0]) {
        str += key + " "
    }
    str += "\n"
    for(var n=0; n<rows.length; n++) {
        var line = ""
        for(var key in rows[n]) {
            line += rows[n][key] + " "
        }
        str += line + "\n"
    }

    $("#results").text(str)
}

function clearPlot(selector) {
    $(selector+" svg").remove()
}

function renderHistogram(rows, selector) {
    console.log('renderHistogram enter')
    if(Object.keys(rows[0]).length == 2) {
        keys = Object.keys(rows[0])
        for(var key in rows[0]) {
            if(key != "count") {
                xkey = key
            }
        }

        console.log(xkey)
        // expects rows like [{xkey: 10, 'count': 100}, ...]
        hist1D(rows, xkey, selector)
    } else if('x' in rows[0] && 'y' in rows[0]) {
        // expects rows like [{'x': 30, 'y': 70, 'count': 100}, ...] 
        hist2D(rows, selector)
    }
}

function addCommas(intNum) {
    return (intNum + '').replace(/(\d)(?=(\d{3})+$)/g, '$1,');
}

function startup() {
    populate_version()
}

function populate_version() {
  var xhr = new XMLHttpRequest();
    xhr.open('GET', '/version')
    var node = document.getElementById('version-info')

    xhr.onload = function() {
      data = JSON.parse(xhr.responseText)
      node.innerHTML = "server: " + data['pilosaversion'] + "<br />demo: " + data['demoversion']
    }
    xhr.send(null)
}

var frames = {
    cab_type: 0,
    pickup_year: 0,
    pickup_month: 0,
    pickup_day: 0,
    pickup_time: 0,
    dist_miles: 0,
    duration_minutes: 0,
    speed_mph: 0,
    passenger_count: 0,
    total_amount_dollars: 0,
    weather_condition: 0,
    temp_f: 0,
    precipitation_inches: 0,
    // precipitation_type: 0,
    pressure_i: 0,
    humidity: 0,
    pickup_elevation: 0,
    drop_elevation: 0,
}

function makeIntersectQuery() {
    indent = "  "
    var toIntersect = [];
    var frame_els = $(".intersect-frame")  // TODO should be able to use this instead of frames dict
    for (var frame in frames) {
        el = $("#" + frame);
        var val = el.val();
        if (!val) {
            continue;
        }
        var toUnion = [];
        for (var i = 0; i < val.length; i++) {
            if (!val[i]) {
                continue;
            }            
            toUnion.push(indent + "Bitmap(frame='" + frame + "',rowID=" + val[i] + ")");
        }
        if (toUnion.length == 1) {
            toIntersect.push(toUnion[0]);
        }
        else if (toUnion.length > 1) {
            toIntersect.push(indent + "Union(\n" + indent+ toUnion.join(",\n" + indent) + "\n" + indent + ")");
        }
        
    }
    if (toIntersect.length > 0) {
        return "Count(Intersect(\n" + toIntersect.join(", \n") + "\n))";
    }
    return "";
}