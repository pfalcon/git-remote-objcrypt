const readline = require('readline')
const log = require('./logger.js')

async function* lines(input) {
  let rl = readline.createInterface({
    input: input,
    terminal: false
  })
  for await (const line of rl) {
    yield line
  }
}

const line = async(input) => {
  for await (const line of lines(input)) {
    return line
  }
}

async function streamToBin(stream) {
    const chunks = [];

    for await (const chunk of stream) {
        chunks.push(Buffer.from(chunk));
    }

    return Buffer.concat(chunks)
}

module.exports = {
  lines: lines,
  line: line,
  streamToBin: streamToBin,
}
