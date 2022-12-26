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

const save = async(dst, tag) => {
  log.profile('save', { level: 'silly' })

  const tagWr = tagWriter(dst)
  for (const [k, v] of Object.entries(_map)) {
    tagWr.stdin.write(`${k} ${v}\n`)
  }
  tagWr.stdin.end()
  const oid = await m.line(tagWr.stdout)

  // update the tag to point to the new object
  gitSync(["update-ref", tag, oid], { cwd : dst })

  log.profile('save', { level: 'silly' })
  return oid
}

module.exports = {
  get: get,
  getKey: getKey,
  set: set,
  verifySet: verifySet,
  save: save,
}
