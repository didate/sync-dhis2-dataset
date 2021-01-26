const axios = require('axios');
const config = require('config')

const dhis2 = axios.create({ baseURL: config.get('dhis2.dhisBaseUrl') });
const query = axios.create({ baseURL: config.get('dhis2.mspBaseUrl') });

dhis2.interceptors.request.use((request) => {
    request.headers = {
        Authorization: config.get('dhis2.token'),
        "Content-Type": "application/json"
    }
    return request;
}, (error) => { return Promise.reject(error) });

query.interceptors.request.use((request) => {
    request.headers = {
        Authorization: config.get('dhis2.token'),
        "Content-Type": "application/json"
    }
    return request;
}, (error) => { return Promise.reject(error) });


exports.dhis2 = dhis2;
exports.query = query;