document.addEventListener('DOMContentLoaded', () => {
    fetch('data/candidates.json')
        .then(response => response.json())
        .then(data => {
            document.getElementById('last-updated').textContent = `最終更新: ${data.updated_at}`;
            renderTable(data.candidates);
        })
        .catch(err => console.error('データ読み込みエラー:', err));
});

function renderTable(candidates) {
    const tbody = document.querySelector('#candidate-table tbody');
    tbody.innerHTML = '';

    candidates.forEach(c => {
        const row = document.createElement('tr');
        
        // 金額フォーマット (単位: 百万円)
        const ncavMillion = (c.ncav / 1000000).toLocaleString(undefined, {maximumFractionDigits: 0});
        const perShare = c.ncav_per_share ? Math.floor(c.ncav_per_share).toLocaleString() : '-';

        row.innerHTML = `
            <td>${c.company}</td>
            <td>${c.submit_date}</td>
            <td class="num">${ncavMillion}</td>
            <td class="num">${perShare}</td>
            <td>${c.flags.length > 0 ? '⚠️ ' + c.flags.join(',') : '✅'}</td>
            <td><a href="${c.link}" target="_blank">書類へ</a></td>
        `;
        tbody.appendChild(row);
    });
}

function downloadCSV() {
    // 簡易的なCSVダウンロード実装
    window.open('data/candidates.json'); // 実際はJSONからCSV変換処理を入れる
}