
module.exports = {
  entry: ['babel-polyfill', './src/index.js'],
  output: {
    filename: 'built/index.js'
  },
  target: 'webworker',
  externals: {
    'datomish.js': 'commonjs datomish.js',
    'sdk/self': 'commonjs sdk/self',
    'sdk/ui/button/action': 'commonjs sdk/ui/button/action',
    'sdk/tabs': 'commonjs sdk/tabs'
  },
  module: {
    loaders: [{
      test: /\.js?$/,
      exclude: /(node_modules)|(wrapper.prefix)/,
      loader: 'babel'
    }]
  }
}
