'use strict';


const path = require('path');
const webpack = require('webpack');


const config = module.exports = {
    context: path.join(__dirname, 'appjs'),
    entry: {
        cms: ['./cms'],
        'rq-table': ['./rq-table'],
    },
    module: {
        loaders: [
            {
                test: /\.js$/,
                exclude: /node_modules/,
                loader: "babel-loader?cacheDirectory",
            },
        ],
    },
    output: {
        filename: 'bundle-[name].js',
        path: path.join(__dirname, 'cubicweb_frarchives_edition', 'data'),
    },
    resolve: {
        alias: {
            lodash: 'lodash-es',
        },
    },
    plugins: [
        new webpack.IgnorePlugin(/^(buffertools)$/), // unwanted "deeper" dependency
        new webpack.DllReferencePlugin({
            context: '.',
            manifest: require('./vendor-manifest.json'),
        }),
    ],
};



if (process.env.NODE_ENV === 'production') {
    // install polyfills for production
    config.plugins.push(
        new webpack.optimize.UglifyJsPlugin(),
        new webpack.DefinePlugin({
            "process.env": {
                NODE_ENV: JSON.stringify('production'),
            },
        })
    );
}
