var webpack = require('webpack');  
module.exports = {  
    entry: {
	app: './js/app.js',
	waldump: './js/waldump.js'
    },
    output: {
	filename: "[name]_bundle.js",
	path: __dirname + '/static'
    },
    module: {
	rules: [
	    {
		test: /\.js?$/,
		exclude: /node_modules/,
		use: {
		    loader: 'babel-loader',
		    options: {
			presets: ['@babel/preset-env']
		    }
		}
	    }
	]
    },
    plugins: [
    ]
};
