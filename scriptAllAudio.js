<<<<<<< HEAD
var height = 15;
var attempt = 4;
var intS = 0;
function scrollToEndPage() {
    if (height < document.body.scrollHeight)
    {
        window.scrollTo(0, height);
        attempt++;
        height = parseInt(height) + attempt;
    }
    else
    {
        clearInterval(intS);
        elementList = document.querySelectorAll(".audio_row__inner")
        superList= [];
        elementList.forEach(function(element) {
            superList.push(element.innerText.replace(/\n/m, ' - ').split(/\n/)[0]);
        });
    }
}
intS = setInterval(scrollToEndPage,10);


////////w8 and after :
=======
var height = 15;
var attempt = 4;
var intS = 0;
function scrollToEndPage() {
    if (height < document.body.scrollHeight)
    {
        window.scrollTo(0, height);
        attempt++;
        height = parseInt(height) + attempt;
    }
    else
    {
        clearInterval(intS);
        elementList = document.querySelectorAll(".audio_row__inner")
        superList= [];
        elementList.forEach(function(element) {
            superList.push(element.innerText.replace(/\n/m, ' - ').split(/\n/)[0]);
        });
    }
}
intS = setInterval(scrollToEndPage,10);


////////w8 and after :
>>>>>>> 03d08ec9312006f32ca4c1d8a18d32fae1fce53e
JSON.stringify(superList)