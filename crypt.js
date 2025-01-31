const stream = require('stream')
const crypto = require('crypto')

const CIPHER = 'aes-256-cbc'
const IVSIZE = 16

class IVReader extends stream.Transform {
  constructor(options) {
    super(options)
    this.iv = Buffer.alloc(0)
  }
  _transform(chunk, encoding, callback) {
    if (this.iv.length == IVSIZE) {
      this.push(chunk)
    } else {
      let nbytes = IVSIZE - this.iv.length
      let ivpart = chunk.slice(0, nbytes)
      this.iv = Buffer.concat([this.iv, ivpart])
      if (this.iv.length == IVSIZE) {
        this.emit('iv', this.iv);
      }
      this.push(chunk.slice(nbytes))
    }
    callback()
  }
}

const decryptStream = (key, input, output, encoding = null) => {
  let options = {}
  if (encoding) {
    // input outputs strings so we can decode them as
    // defaultEncoding
    input.setEncoding('utf8')
    options = {defaultEncoding: encoding, decodeStrings: true}
  }
  let ivr = new IVReader(options)
  input.pipe(ivr)
  ivr.on('iv', (iv) => {
    let deci = crypto.createDecipheriv(CIPHER, key, iv)
    ivr.pipe(deci).pipe(output)
  })
}

const decryptStreamIn = (key, input, encoding = null) => {
  let options = {}
  if (encoding) {
    // input outputs strings so we can decode them as
    // defaultEncoding
    input.setEncoding('utf8')
    options = {defaultEncoding: encoding, decodeStrings: true}
  }
  let ivr = new IVReader(options)
  input.pipe(ivr)
  return new Promise((resolve) => {ivr.on('iv', (iv) => {
    let deci = crypto.createDecipheriv(CIPHER, key, iv)
    ivr.pipe(deci)
    resolve(deci)
    })
  })
}

const encryptStream = (key, input, output, encoding = null) => {
  let iv = crypto.randomBytes(IVSIZE)
  // use passthrough to recode stream joining iv + ciph
  let p = new stream.PassThrough(encoding ? {encoding: encoding} : {})
  // write the iv before we do anything
  p.write(iv, () => {
    let ciph = crypto.createCipheriv(CIPHER, key, iv)
    input.pipe(ciph).pipe(p).pipe(output)
  })
}

const cryptString = (pher, key, input, encoding = null) => {
  let output = new stream.PassThrough()
  pher(key, stream.Readable.from(input), output, encoding)
  let result = ""
  output.on('data', (d) => result += d)
  return new Promise((resolve) => output.on('end', () => {
    resolve(result)}))
}

const decryptString = (key, input, encoding = null) => {
  return cryptString(decryptStream, key, input, encoding)
}

const encryptString = (key, input, encoding = null) => {
  return cryptString(encryptStream, key, input, encoding)
}

const cryptBin = (pher, key, input) => {
  let output = new stream.PassThrough()
  pher(key, stream.Readable.from(input), output, null)
  let result = []
  output.on('data', (d) => result.push(d))
  return new Promise((resolve) => output.on('end', () => {
    resolve(Buffer.concat(result))}))
}

const decryptBin = (key, input, encoding = null) => {
  if (encoding == "hex") {
    input = input.toString()
    input = Buffer.from(input, "hex")
  }
  return cryptBin(decryptStream, key, input)
}

const encryptBin = async(key, input, encoding = null) => {
  let r = cryptBin(encryptStream, key, input)
  if (encoding == "hex") r = (await r).toString("hex")
  return r
}

module.exports = {
  encryptStream,
  decryptStream,
  decryptStreamIn,
  decryptString,
  encryptString,
  decryptBin,
  encryptBin,
}