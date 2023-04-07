const form = document.querySelector('#search');
const input = document.querySelector('#input');
const result = document.querySelector('#result')

form.addEventListener('submit', (event) => {
    event.preventDefault();
    
    const uid = input.value;

    getOrder(uid, showData);
});

function getOrder(uid, callback) {
    const xhr = new XMLHttpRequest();
    xhr.open('GET', `/cache/${uid}`);

    xhr.onload = () => {
    if (xhr.status === 200) {
        callback(xhr.status, xhr.response);
    } else {
        callback(404, xhr.response);
    }
    };
    xhr.send();
}

function showData(status, data) {
    // Удалить все ранее созданные элементы
    while (result.firstChild) {
        result.removeChild(result.firstChild);
    }

    if (status != 200) {
        err_containter = document.createElement('p');
        err_text = document.createTextNode(`Запись не найдена: ${data}`);
        err_containter.appendChild(err_text);
        result.appendChild(err_containter);
        return
    }
    let parsed = JSON.parse(data) 
    $('#result').jsonViewer(parsed);
}
