const express = require('express');
const fs = require('fs');
const csv_parser = require('csv-parser');
const path = require("path")

const app = express();
const port = 3000;

app.get('/data/userlogs', async (req, res) => {
  const csvFilePath = path.join(__dirname, "resources/data/training_set/log_mini.csv");

  res.setHeader('Content-Type', 'application/json');

  const data = [];

  fs.createReadStream(csvFilePath)
    .pipe(csv_parser())
    .on('data', (row) => {
      data.push(row);
    })
    .on('end', async () => {
      for (const row of data) {
        res.write(JSON.stringify(row) + '\n');

        await new Promise((resolve) => setTimeout(resolve, 2000));
      }
      res.end();
    });
});


app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});
