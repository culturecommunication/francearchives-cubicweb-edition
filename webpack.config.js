'use strict'

const path = require('path')
const webpack = require('webpack')

const config = (module.exports = {
    context: path.join(__dirname, 'appjs'),
    entry: {
        cms: ['./cms'],
        'rq-table': ['./rq-table'],
        edition: ['./edition'],
    },
    module: {
        rules: [
            {
                test: /\.js$/,
                exclude: /node_modules/,
                loader: 'babel-loader',
                options: {
                    cacheDirectory: true,
                },
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
        new webpack.IgnorePlugin({
            resourceRegExp: /^(buffertools)$/,
        }), // unwanted "deeper" dependency
        new webpack.DllReferencePlugin({
            context: __dirname,
            manifest: require('./vendor-manifest.json'),
        }),
    ],
})

if (process.env.NODE_ENV === 'production') {
    // install polyfills for production
    config.plugins.push(
        new webpack.DefinePlugin({
            'process.env': {
                NODE_ENV: JSON.stringify('production'),
            },
        }),
    )
}
