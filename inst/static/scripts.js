import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@latest/+esm";

let db;

async function initDuckDB() {
  try {
    // receive the bundles of files required to run duckdb in the browser
    // this is the compiled wasm code, the js and worker scripts
    // worker scripts are js scripts ran in background threads (not the same thread as the ui)
    const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
    // select bundle is a function that selects the files that will work with your browser
    const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

    // creates storage and an address for the main worker
    const worker_url = URL.createObjectURL(
      new Blob([`importScripts("${bundle.mainWorker}");`], {
        type: "text/javascript",
      })
    );

    // creates the worker and logger required for an instance of duckdb
    const worker = new Worker(worker_url);
    const logger = new duckdb.ConsoleLogger();
    db = new duckdb.AsyncDuckDB(logger, worker);

    // loads the web assembly module into memory and configures it
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

    // revoke the object url now no longer needed
    URL.revokeObjectURL(worker_url);
    console.log("DuckDB-Wasm initialized successfully.");

    // Ensure the SQL query interface is visible on page load even if there are no tables yet
    try {
      const queryEntryDiv = document.getElementById("queryEntryDiv");
      if (queryEntryDiv) queryEntryDiv.style.display = "block";
      const queryResultsDiv = document.getElementById("queryResultsDiv");
      if (queryResultsDiv) queryResultsDiv.style.display = "block";
    } catch (e) {
      console.warn("Could not update UI visibility after DB init:", e);
    }
  } catch (error) {
    console.error("Error initializing DuckDB-Wasm:", error);
  }
}

async function uploadTable() {
  try {
    const fileInput = document.getElementById("fileInput");
    const fileUrlInput = document.getElementById("fileUrlInput");
    const file = fileInput.files ? fileInput.files[0] : null;
    let arrayBuffer, fileName;
    let isRemote = false;

    if (file) {
      arrayBuffer = await file.arrayBuffer();
      fileName = file.name;
    } else if (fileUrlInput && fileUrlInput.value) {
      // For remote URLs, delegate reading to DuckDB via httpfs instead of fetching here
      isRemote = true;
      // strip query string if present when deriving a filename
      fileName = (fileUrlInput.value.split("/").pop() || "remote_file").split("?")[0];
    } else {
      alert("Please select a file or enter a file URL.");
      return;
    }

    const tableNameInput = document.getElementById("tableNameInput");
    const tableName = tableNameInput.value;
    if (!tableName) {
      alert("Please enter a valid table name.");
      return;
    }

    if (!db) {
      console.error("DuckDB-Wasm is not initialized");
      return;
    }
    console.log("File loaded:", fileName);

    const conn = await db.connect();
    console.log("Database connection established");

    // Extension install/load (if specified by user)
    const extensionInput = document.getElementById("extensionInput");
    if (extensionInput && extensionInput.value.trim()) {
      const extName = extensionInput.value.trim();
      try {
        await conn.query(`INSTALL ${extName} FROM 'https://community-extensions.duckdb.org';`);
        await conn.query(`LOAD ${extName};`);
        console.log(`Extension ${extName} installed and loaded.`);
      } catch (error) {
        console.error("Error installing/loading extension:", error);
      }
    }

    // If the source is a remote URL, ensure httpfs is available so DuckDB can read directly from the URL
    if (isRemote) {
      try {
        // INSTALL may fail if already installed; ignore install error
        await conn.query(`INSTALL httpfs FROM 'https://community-extensions.duckdb.org';`);
      } catch (e) {
        console.warn('httpfs install may have failed or already installed:', e);
      }
      try {
        await conn.query(`LOAD httpfs;`);
        console.log('httpfs loaded to allow DuckDB to read remote URLs');
      } catch (e) {
        console.error('Error loading httpfs extension:', e);
      }
    }

    const fileType = fileName.split(".").pop()?.toLowerCase() || "";

    // For remote files, delegate to DuckDB by using the HTTP URL directly in the read_* call.
    // For local uploads, register the buffer in the virtual FS and read from the virtual path.
    let sourcePath;
    if (isRemote) {
      sourcePath = fileUrlInput.value;
    } else {
      sourcePath = `/${fileName}`;
      await db.registerFileBuffer(sourcePath, new Uint8Array(arrayBuffer));
    }

    if (fileType === "csv" || fileType === "parquet" || fileType === "json") {
      let query = "";
      if (fileType === "csv") {
        query = `CREATE TABLE '${tableName}' AS FROM read_csv_auto('${sourcePath}', header = true)`;
      } else if (fileType === "parquet") {
        query = `CREATE TABLE '${tableName}' AS FROM read_parquet('${sourcePath}')`;
      } else if (fileType === "json") {
        query = `CREATE TABLE '${tableName}' AS FROM read_json_auto('${sourcePath}')`;
      }

      await conn.query(query);
      updateTableList();
    } else {
      console.log("Invalid file type: ", fileType);
    }
    await conn.close();
  } catch (error) {
    console.error("Error processing file or querying data:", error);
  }
}

async function updateTableList() {
  console.log("now running updateTableList");
  try {
    if (!db) {
      console.error("DuckDB-Wasm is not initialized");
      return;
    }

    const conn = await db.connect();
    console.log("Database connection established");
    const query = `SELECT table_name as TABLES FROM information_schema.tables WHERE table_schema = 'main';`;
    const showTables = await conn.query(query);

    const rowCount = showTables.numRows;
    const tablesDiv = document.getElementById("tablesDiv");
    const queryEntryDiv = document.getElementById("queryEntryDiv");
    console.log("rowCount: ", rowCount);

    // Always keep the SQL query entry visible so users can run queries at any time.
    if (rowCount === 0) {
      if (tablesDiv) tablesDiv.style.display = "none";
      if (queryEntryDiv) queryEntryDiv.style.display = "block";
    } else {
      if (tablesDiv) tablesDiv.style.display = "block";
      if (queryEntryDiv) queryEntryDiv.style.display = "block";
      arrowToHtmlTable(showTables, "tablesTable");
    }

    await conn.close();
    console.log("Database connection closed");
  } catch (error) {
    console.error("Error processing file or querying data:", error);
  }
}

function arrowToHtmlTable(arrowTable, htmlTableId) {
  // Log the arrowTable to see if it's valid
  console.log("arrowTable:", arrowTable);

  if (!arrowTable) {
    console.error("The arrowTable object is invalid or null.");
    return;
  }

  const tableSchema = arrowTable.schema.fields.map((field) => field.name);
  console.log("tableSchema:", tableSchema); // Log the schema

  const tableRows = arrowTable.toArray();
  console.log("tableRows:", tableRows); // Log the rows

  let htmlTable = document.getElementById(htmlTableId);
  if (!htmlTable) {
    console.error(`Table with ID ${htmlTableId} not found in the DOM.`);
    return;
  }

  htmlTable.innerHTML = "";
  let tableHeaderRow = document.createElement("tr");
  htmlTable.appendChild(tableHeaderRow);
  tableSchema.forEach((tableColumn) => {
    let th = document.createElement("th");
    th.innerText = tableColumn;
    tableHeaderRow.appendChild(th);
  });

  tableRows.forEach((tableRow) => {
    let tr = document.createElement("tr");
    htmlTable.appendChild(tr);
    tableSchema.forEach((tableColumn) => {
      let td = document.createElement("td");
      td.innerText = tableRow[tableColumn];
      tr.appendChild(td);
    });
  });
}

async function runQuery() {
  const queryInput = document.getElementById("queryInput");
  let query = queryInput.value;
  const queryResultsDiv = document.getElementById("queryResultsDiv");

  // Make sure the results div is visible before populating it
  queryResultsDiv.style.display = "block";

  const lastQueryDiv = document.getElementById("lastQueryDiv");
  lastQueryDiv.innerHTML = query;

  const resultTable = document.getElementById("resultTable");
  const resultErrorDiv = document.getElementById("resultErrorDiv");

  try {
    if (!db) {
      console.error("DuckDB-Wasm is not initialized");
      return;
    }

    const conn = await db.connect();
    console.log("Database connection established");

    const result = await conn.query(query);
    arrowToHtmlTable(result, "resultTable");
    await updateTableList();
    queryResultsDiv.style.display = "block";
    resultTable.style.display = "block";
    resultErrorDiv.style.display = "none";
    resultErrorDiv.innerHTML = "";

    await conn.close();
    console.log("Database connection closed");
  } catch (error) {
    resultTable.style.display = "none";
    resultTable.innerHTML = "";
    resultErrorDiv.style.display = "block";
    resultErrorDiv.innerHTML = error;
    console.error("Error processing file or querying data:", error);
  }
}

// Add a function to load/install extensions without creating a table
async function loadExtension() {
  try {
    const extensionInput = document.getElementById("extensionInput");
    const extName = extensionInput && extensionInput.value ? extensionInput.value.trim() : "";
    if (!extName) {
      alert("Please enter an extension name to load (e.g. httpfs, h3, icu).");
      return;
    }

    if (!db) {
      console.error("DuckDB-Wasm is not initialized");
      alert("DuckDB not initialized yet. Please wait a moment and try again.");
      return;
    }

    const conn = await db.connect();
    try {
      // Try installing from community extensions (may already be present)
      await conn.query(`INSTALL ${extName} FROM 'https://community-extensions.duckdb.org';`);
      console.log(`Extension ${extName} installed.`);
    } catch (installErr) {
      console.warn(`Install may have failed or extension already present: ${installErr}`);
    }

    try {
      await conn.query(`LOAD ${extName};`);
      console.log(`Extension ${extName} loaded.`);
      alert(`Extension ${extName} loaded successfully.`);
    } catch (loadErr) {
      console.error(`Error loading extension ${extName}:`, loadErr);
      alert(`Error loading extension ${extName}: ${loadErr}`);
    }

    await conn.close();
  } catch (err) {
    console.error("Unexpected error in loadExtension:", err);
    alert(`Unexpected error: ${err}`);
  }
}

// Initialize DuckDB on page load
document.addEventListener("DOMContentLoaded", () => {
  initDuckDB();

  window.uploadTable = uploadTable;
  window.runQuery = runQuery;
  window.loadExtension = loadExtension;
});
