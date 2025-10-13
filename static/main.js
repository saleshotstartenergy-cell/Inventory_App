$(document).ready(function() {
    // --- Toggle sections ---
    $("#show-sales").click(function() {
        $("#sales-section").show();
        $("#stock-section").hide();
        $(this).addClass("active");
        $("#show-stock").removeClass("active");
    });

    $("#show-stock").click(function() {
        $("#stock-section").show();
        $("#sales-section").hide();
        $(this).addClass("active");
        $("#show-sales").removeClass("active");
    });

    // --- Auto-refresh functions ---
    function renderTable(containerId, columns, data, clickableCol, onClickCallback) {
        if (data.length === 0) {
            document.getElementById(containerId).innerHTML = "<p>No data available</p>";
            return;
        }

        let html = "<table><thead><tr>";
        columns.forEach(col => html += `<th>${col}</th>`);
        html += "</tr></thead><tbody>";

        data.forEach(row => {
            html += "<tr>";
            columns.forEach(col => {
                if (col === clickableCol && onClickCallback) {
                    html += `<td class="clickable" data-key="${row[col]}">${row[col]}</td>`;
                } else {
                    html += `<td>${row[col] !== undefined ? row[col] : ''}</td>`;
                }
            });
            html += "</tr>";
        });

        html += "</tbody></table>";
        document.getElementById(containerId).innerHTML = html;

        if (clickableCol && onClickCallback) {
            $(`#${containerId} .clickable`).click(function() {
                const key = $(this).data("key");
                onClickCallback(key);
            });
        }
    }

    function fetchSales() {
        $.getJSON('/api/sales', function(data) {
            renderTable("sales", ["company", "product_company", "total_sales"], data, "product_company", fetchSalesItems);
        });
    }

    function fetchSalesItems(productCompany) {
        $.getJSON(`/api/sales-items?product_company=${productCompany}`, function(data) {
            renderTable("sales-items", ["item_name", "quantity", "sales_amount"], data);
        });
    }

    function fetchStock() {
        $.getJSON('/api/stock', function(data) {
            renderTable("stock", ["company", "item_name", "rate", "value", "quantity"], data, "company", fetchStockItems);
        });
    }

    function fetchStockItems(company) {
        $.getJSON(`/api/stock-items?company=${company}`, function(data) {
            renderTable("stock-items", ["item", "quantity", "rate", "value"], data);
        });
    }

    // --- Auto-refresh every 5 seconds ---
    setInterval(() => {
        fetchSales();
        fetchStock();
    }, 5000);

    // Initial load
    fetchSales();
    fetchStock();
});
