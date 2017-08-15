var HttpClient = function() {

  this.get = function(aUrl, aCallback) {
    var anHttpRequest = new XMLHttpRequest();
    anHttpRequest.onreadystatechange = function() {
      if (anHttpRequest.readyState == 4 && anHttpRequest.status == 200)
        aCallback(anHttpRequest.responseText);
    }
    anHttpRequest.open("GET", aUrl, true);
    anHttpRequest.send(null);
  }

  this.post = function(aUrl, body, aCallback) {
    var anHttpRequest = new XMLHttpRequest();
    anHttpRequest.onreadystatechange = function() {
      if (anHttpRequest.readyState == 4 && anHttpRequest.status == 200)
        aCallback(anHttpRequest.responseText);
    }
    anHttpRequest.open("POST", aUrl, true);
    anHttpRequest.setRequestHeader("Content-Type", "application/json");
    anHttpRequest.send(JSON.stringify(body));
  }
}

var client = new HttpClient();
var dataset = {};
var block = {};

$(document).ready(function() {
  $("#get-data").click(function() {
    sname = $("#source").val();
    client.get('http://' + sname + '/tfc', function(response) {
      var catalog = JSON.parse(response);
      dataset = {};
      block = {};
      $.each(catalog, function(index) {
        dataset[catalog[index].dataset] = [];
      })
      $.each(catalog, function(index) {
        dataset[catalog[index].dataset].push(catalog[index].block);
        block[catalog[index].block] = [];
      })
      $.each(catalog, function(index) {
        block[catalog[index].block].push(catalog[index].lfn);
      })
      var datasetKey = Object.keys(dataset)
      $('#dataset').find('option').remove().end()
        .append('<option value="select">select</option>')
        .val('select');
      $.each(datasetKey, function(index) {
        var option = $('<option value="' + datasetKey[index] + '">' + datasetKey[index] + '</option>');
        $('#dataset').append(option);
        $('#dataset').trigger("chosen:updated");
      })
    })
  });
  $('#dataset').on('change', function(e) {
    var optionSelected = $("option:selected", this);
    var valueSelected = this.value;
    var blockKeys = dataset[valueSelected]
    $('#block').find('option').remove().end()
      .append('<option value="select">select</option>')
      .val('select');
    $.each(blockKeys, function(index) {
      var option = $('<option value="' + blockKeys[index] + '">' + blockKeys[index] + '</option>');
      $('#block').append(option);
      $('#block').trigger("chosen:updated");
    })
  });
  $('#block').on('change', function(e) {
    var optionSelected = $("option:selected", this);
    var valueSelected = this.value;
    var files = block[valueSelected]
    $('#file').find('option').remove().end()
      .append('<option value="select">select</option>')
      .val('select');
    $.each(files, function(index) {
      var option = $('<option value="' + files[index] + '">' + files[index] + '</option>');
      $('#file').append(option);
      $('#file').trigger("chosen:updated");
    })
  });
  $('#request-form').on('submit', function(e) {
    var dataset = $('#dataset').val();
    var block = $('#block').val();
    var file = $('#file').val();

    if(dataset=="select"){
      dataset = "";
    }
    if(block=="select"){
      block = "";
    }
    if(file=="select"){
      file = "";
    }
    var reqArr = [];
    reqArr.push({
      SrcUrl: 'http://' + $("#source").val(),
      DstUrl: 'http://' + $("#destination").val(),
      File: file,
      Block: block,
      Dataset: dataset
    });
    client.post('http://'+$("#main-agent").val()+'/request', reqArr, function(response) {
      console.log(response)
    })
  });
});
