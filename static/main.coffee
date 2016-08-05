
handle = (serp, status, resp)->
  template = $('#template').html()
  Mustache.parse(template)
  rendered = Mustache.render(template, serp)
  $("#serp").html(rendered)

window.search = ->
    q =  $('#q').val()
    $.getJSON('/api', {q:q}, handle)
    true
