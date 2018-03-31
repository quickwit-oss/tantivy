var themes = document.getElementById("theme-choices");
var themePicker = document.getElementById("theme-picker");
themePicker.onclick = function() {
    if (themes.style.display === "block") {
        themes.style.display = "none";
        themePicker.style.borderBottomRightRadius = "3px";
        themePicker.style.borderBottomLeftRadius = "3px";
    } else {
        themes.style.display = "block";
        themePicker.style.borderBottomRightRadius = "0";
        themePicker.style.borderBottomLeftRadius = "0";
    }
};
["dark","light"].forEach(function(item) {
    var but = document.createElement('button');
    but.innerHTML = item;
    but.onclick = function(el) {
        switchTheme(currentTheme, mainTheme, item);
    };
    themes.appendChild(but);
});
