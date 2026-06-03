// vim: set ts=2 sw=2 tw=99 et:
function ToolTip(owner, id, contents)
{
  this.owner = owner;
  this.id = id;
  this.contents = contents;
  this.elt = null;
}

ToolTip.prototype.draw = function(x, y)
{
  var elt = $("<div class='tooltip'></div>");
  elt.html(this.contents);
  this.present(x, y, elt);
}

ToolTip.prototype.present = function(x, y, elt)
{
  var tipWidth = 165;
  var tipHeight = 75;
  var xOffset = -10;
  var yOffset = 15;

  var ie = document.all && !window.opera;
  var iebody = (document.compatMode == 'CSS1Compat')
               ? document.documentElement
               : document.body;
  var scrollLeft = ie ? iebody.scrollLeft : window.pageXOffset;
  var scrollTop = ie ? iebody.scrollTop : window.pageYOffset;
  var docWidth = ie ? iebody.clientWidth - 15 : window.innerWidth - 15;
  var docHeight = ie ? iebody.clientHeight - 15 : window.innerHeight - 8;
  var y = (y + tipHeight - scrollTop > docHeight)
          ? y - tipHeight - 5 - (yOffset * 2)
          : y; // account for bottom edge.

  // Account for the right edge.
  elt.css({ top: y + yOffset });

  if (x + tipWidth - scrollLeft > docWidth)
    elt.css({ right: docWidth - x + xOffset });
  else
    elt.css({ left: x + xOffset });

  this.elt = elt;
  this.elt.appendTo('body').fadeIn(200);
}

ToolTip.prototype.remove = function()
{
  this.elt.remove();
  this.elt = null;
}
