var HttpClient = function() {

	this.get = function(aUrl, aCallback) {
		var anHttpRequest = new XMLHttpRequest();
		anHttpRequest.onreadystatechange = function() {
			if (anHttpRequest.readyState == 4 && anHttpRequest.status == 200)
			aCallback(anHttpRequest.responseText);
		}
		anHttpRequest.open( "GET", aUrl, true );
		anHttpRequest.send( null );
	}

	this.post = function(aUrl, body, aCallback) {
		var anHttpRequest = new XMLHttpRequest();
		anHttpRequest.onreadystatechange = function() {
			if (anHttpRequest.readyState == 4 && anHttpRequest.status == 200)
				aCallback(anHttpRequest.responseText);
		}
		anHttpRequest.open( "POST", aUrl, true );
		anHttpRequest.setRequestHeader("Content-Type", "application/json");
		anHttpRequest.send( JSON.stringify(body) );
	}
}

var client = new HttpClient();

$(document).ready(function () {

	renderRequest("pending");

	$('.rsubmit').on('click', function () {
		var $table = $('table');
		var actionArr = [];
		$('table').each(function() {
    	var id = $(this).find(".id").html();
			var action = $(this).find(".action").val();
			if(action=="transfer"){
				actionArr.push({
          action: "pull" + action,  // TODO: make it configurable.
          request: {id: parseInt(id)}
        });
			}else if(action=="delete"){
				actionArr.push({
          action: action,
          request: {id: parseInt(id)}
        });
			}else{
				console.log(action);
			}
 		});
    if(actionArr.length>0) {
      client.post('http://'+window.location.host+'/action', actionArr, function(response) {
        console.log(response)
      })
    }
	});

  $('.btn-filter').on('click', function () {
    var $target = $(this).data('target');
		renderRequest($target);
	})
});

function renderRequest(type) {
	client.get('http://' + window.location.host + '/list?type=' + type, function(response) {
		var tRequests = JSON.parse(response);
		$('.table tr').css('display', 'none');
		$.each(tRequests, function(index) {
			var rid = 'request-'+tRequests[index].id;
			var html = '<div class="reqdiv" id="div-'+rid+'">';
			html += '<b>Request:</b> <span class="id">'+tRequests[index].id+'</span>&nbsp;';
			html += '<b>Status:</b> <span style="color:'+genColor(tRequests[index].id)+';background-color:#fff;padding:3px;">'+tRequests[index].status+'</span>&nbsp;';
			html += '<b>Priority:</b> '+tRequests[index].priority+'&nbsp;';
			html += '<div class="lift">'
			html += '<nav class="navbar navbar-left">';
			html += '<span></span>'
			html += '</nav>';
			html += '<nav class="navbar navbar-right">'
			html += '<select class="action"> <option value="none">None</option> <option value="delete">delete</option> <option value="transfer">transfer</option> </select>';
			html += '</nav>'
			html += '</div>';
			html += '<br/><b>Source:</b> '+tRequests[index].srcUrl+'&nbsp;';
			html += '<b>Dest:</b> '+tRequests[index].dstUrl+'&nbsp;<br/>';
			html += '<b>Block:</b> '+tRequests[index].block+'&nbsp;';
			html += '<b>Dataset:</b> '+tRequests[index].dataset+'&nbsp;';
			html += '<b>File:</b> '+tRequests[index].file;
			html += '<hr/></div>'
			tRow = $('<tr>');
			tRow.append(html)
			$('table').append(tRow);
		});
	});
}

function genColor(s) {
	var color = '#'+s.toString().substr(0,6);
	return color;
}
