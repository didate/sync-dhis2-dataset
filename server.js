const { dhis2source, dhis2dest } = require('./dhis2');
const fs = require('fs');
const csv = require('csv-parser');
const moment = require('moment');  
const config = require('config');
const { exit } = require('process');

const dataset = config.get("dataset");


const run = async () => {

    
    const periods = [
        { year: 2015, start: 1, end: 12 },
        { year: 2016, start: 1, end: 12 },
        { year: 2017, start: 1, end: 12 },
        { year: 2018, start: 1, end: 12 },
        { year: 2019, start: 1, end: 12 },
        { year: 2020, start: 1, end: 12 },
        { year: 2021, start: 1, end: 12 },
    ]
    const orgunits = [];

    fs.createReadStream('./orgunit.csv').pipe(csv()).on('data', (row) => {
        orgunits.push(row);
    }).on('end', async () => {
        console.log('Starting Sync .....');

        for (let i = 0; i < periods.length; i++) {
            const period = periods[i];
            let debut = period.start    
            while (debut <= period.end) {
                const month = `${debut}`.padStart(2, '0');
                const currentPeriod = `${period.year}${month}`;
                for (let index = 0; index < orgunits.length; index++) {
                    const orgunit = orgunits[index];
                    console.log(`====================================================`);
                    console.log(`Current organisation unit : ${orgunit.uid}`);
                    console.log(`Current period : ${currentPeriod}`);

                    try {
                        console.log("Get Dataset values from dhis2 source .....")
                        let dataSetValues = await getDataSetValuesFromSource(orgunit.uid, currentPeriod);
                        if (dataSetValues && dataSetValues.dataValues && dataSetValues.dataValues.length > 0) {
                            delete dataSetValues['orgUnit']
                            delete dataSetValues['dataSet']
                            delete dataSetValues['period']
                            console.log("Send Dataset values to dhis2 destination .....")
                            await postDataSetValuesToDestination(dataSetValues, orgunit.uid, currentPeriod);
                        }
                    } catch (error) {
                        console.log(error);
                        logNOTOK(orgunit.uid, currentPeriod);
                    }
                }
                logOK(currentPeriod)
                debut = debut +1;
            }
        }

        console.log('End Sync .....');
    });
}

const getDataSetValuesFromSource = async (orgunit , currentPeriod) => {
    const url = `/dataValueSets?orgUnit=${orgunit}&dataSet=${dataset}&period=${currentPeriod}`;
    const datavaluesfromdhis = await dhis2source.get(url).catch((error) => {
        console.log(error);
        logNOTOK(orgunit, currentPeriod);
        return null;
    });

    return datavaluesfromdhis ? datavaluesfromdhis.data : null;
}

const postDataSetValuesToDestination = async (dataSetValues , orgunit, currentPeriod) => {

    await dhis2dest.post('/dataValueSets?importStrategy=CREATE_AND_UPDATE', JSON.stringify(dataSetValues)).catch((error) => {
        console.log(error)
        logNOTOK(orgunit, currentPeriod)
    })
}


const logOK = (currentPeriod) =>{
    const date = new Date();
    fs.writeFile(`./logs/logs_${date.getFullYear()}${date.getMonth()}${date.getDate()}_OK.log`, `${currentPeriod}\n`, { flag: 'a+' }, err => { })
}

const logNOTOK = (orgunit , currentPeriod) =>{
    const date = new Date();
    fs.writeFile(`./logs/logs_${date.getFullYear()}${date.getMonth()}${date.getDate()}_not_OK.log`, `${orgunit} , ${currentPeriod} \n`, { flag: 'a+' }, err => { });
    process.exit(-1)
}


run();