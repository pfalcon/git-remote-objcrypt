const { git, gitSync } = require('./git.js')
const m = require('./misc.js')
const log = require('./logger.js')
// TODO v2: version the tag file ... ie give each a parent

let _map = null
let _rev_map = null

const tagReadStream = (dst, tag) => {
  // verify ref exists
  let verfiyRef = gitSync(["show-ref", "--verify", "-q", tag], 
    { cwd : dst, ignoreErr: true})
  if (verfiyRef.status != 0) {
    return null
  }

  return git(["cat-file", "blob", tag], { cwd : dst }).stdout
}

const loadMap = async(dst, tag) => {
    log.profile(`loadMap`, { level: 'silly' });
    let tagRdSt = tagReadStream(dst, tag)
    if (tagRdSt === null) {
      log.verbose("no map ref %s exists in %s", tag, dst)
      return false
    }
    _map = {}
    _rev_map = {}
    for await (const line of m.lines(tagRdSt)) {
      let [lkey, value] = line.split(" ")
      _map[lkey] = value
      _rev_map[value] = lkey
    }
    log.verbose("loaded objid map")
    log.profile(`loadMap`, { level: 'silly' });
    return true
}

const get = async(dst, tag, key) => {
  if (_map === null) {
    if (!await loadMap(dst, tag)) return
  }

  if (!(key in _map)) log.verbose("key %s not found", key)
  return _map[key]
}

const getKey = async(dst, tag, val) => {
  if (_rev_map === null) {
    if (!await loadMap(dst, tag)) return
  }

  if (!(val in _rev_map)) log.verbose("val %s not found", val)
  return _rev_map[val]
}

const set = async(dst, tag, key, val) => {
  log.silly("refmap: set %s->%s", key, val)
  if (_map === null) {
    if (!await loadMap(dst, tag)) {
      _map = {}
      _rev_map = {}
    }
  }

  _map[key] = val
  _rev_map[val] = key
}

const verifySet = async(dst, tag, key, val) => {
  log.silly("refmap: verifySet %s->%s", key, val)
  if (_map === null) {
    log.error("expected refmap to be loaded")
    throw Error()
  }

  if (_map[key] !== val) {
    log.error("expected mapping not in refmap")
    throw Error()
  }
  if (_rev_map[val] !== key) {
    log.error("expected reverse mapping not in refmap")
    throw Error()
  }
}

const tagWriter = (dst) => {
  return git(["hash-object","-w", "--stdin"], { cwd : dst })
}

// expected format of a line is `${key} ${value}`
const insert = async(dst, tag, lines) => { 
  log.profile(`insert`, { level: 'silly' })

  lines.sort()
  tagWr = tagWriter(dst)
  tagRdSt = tagReadStream(dst, tag)
  if (tagRdSt) {
    for await (const line of m.lines(tagRdSt)) {
      // if any new lines go before line, write them out
      while (lines.length && line > lines[0]) {
        tagWr.stdin.write(`${lines.shift()}\n`)
      }
      // occasionally we are rewrite a line
      if (line == lines[0]) {
        lines.shift()
        log.warn("dup line %s", line)
      }

      tagWr.stdin.write(`${line}\n`)
    }
  }

  // write remaining lines
  for (const l of lines) {
    tagWr.stdin.write(`${l}\n`)
  }

  tagWr.stdin.end()
  log.profile(`insert`, { level: 'silly' });
  return await m.line(tagWr.stdout)
}

const update = async(dst, tag, kvs) => {
  // write kvs to the file
  let entries = Object.entries(kvs).map(kv => {
    return `${kv[0]} ${kv[1]}`
  })
  oid = await insert(dst, tag, entries)
  
  // update the tag to point to the new object
  gitSync(["update-ref", tag, oid], { cwd : dst })
  return oid
}

module.exports = {
  get: get,
  getKey: getKey,
  set: set,
  verifySet: verifySet,
  update: update
}
