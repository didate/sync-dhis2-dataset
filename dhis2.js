const axios = require('axios');
const config = require('config')

const dhis2source = axios.create({ baseURL: config.get('dhis2.source') });
const dhis2dest = axios.create({ baseURL: config.get('dhis2.destination') });

dhis2source.interceptors.request.use((request) => {
    request.headers = {
        Authorization: config.get('dhis2.token'),
        "Content-Type": "application/json"
    }
    return request;
}, (error) => { return Promise.reject(error) });

dhis2dest.interceptors.request.use((request) => {
    request.headers = {
        Authorization: config.get('dhis2.token'),
        "Content-Type": "application/json"
    }
    return request;
}, (error) => { return Promise.reject(error) });


exports.dhis2source = dhis2source;
exports.dhis2dest = dhis2dest;