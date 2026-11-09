let customConfig = [];
let hasIgnoresFile = true;
try {
  require.resolve('./eslint.ignores.js');
} catch {
  hasIgnoresFile = false;
}

if (hasIgnoresFile) {
  const ignores = require('./eslint.ignores.js');
  customConfig = [{ignores}];
}

module.exports = [...customConfig, ...require('gts')];
