const { dhis2source, dhis2dest } = require('./dhis2');
const fs = require('fs');
const csv = require('csv-parser');
const moment = require('moment');  
const cron = require("node-cron");
const config = require('config');

const dataset = config.get("dataset");




const run = async () => {

    
    const periods = getLastFewsWeek();
    const orgunits = [];

    fs.createReadStream('./orgunit.csv').pipe(csv()).on('data', (row) => {
        orgunits.push(row);
    }).on('end', async () => {
        console.log('Debut du traitement .....');

        for (let i = 0; i < periods.length; i++) {
                const currentWeek = periods[i];

            for (let index = 0; index < orgunits.length; index++) {
                const orgunit = orgunits[index];

                try {
                    let dataSetValues = await getDataSetValuesFromSource(orgunit.uid, currentWeek);
                    if (dataSetValues && dataSetValues.dataValues && dataSetValues.dataValues.length > 0) {
                        //delete dataSetValues['completeDate']
                        
                        postDataSetValuesToDestination(dataSetValues, orgunit.uid, currentWeek);

                        const completeness = await getDataSetCompletenessFromSource(orgunit.uid, currentWeek); 

                        if (completeness && completeness.data ) {
                            postDataSetCompletenessToDestination(completeness.data);
                        }

                    }
                } catch (error) {
                   
                    logNOTOK(orgunit.uid, currentWeek);
                }
            }
            logOK(currentWeek)
        }

        console.log('Fin du traitement .....');
    });
}


const getLastFewsWeek = (numberOfWeek=4) =>{
    index =1 ;
    const weeks = [];
    while(index <= numberOfWeek){
        const period = moment().add(-(index++), 'w');
        weeks.push(`${period.year()}W${period.isoWeek()}`);
    }
    return weeks;
}


const getDataSetValuesFromSource = async (orgunit , week) => {
    const datavaluesfromdhis = await dhis2source.get(`/dataValueSets?orgUnit=${orgunit}&dataSet=${dataset}&period=${week}`).catch((error) => {
        logNOTOK(orgunit, week);
        return null;
    });

    return datavaluesfromdhis ? datavaluesfromdhis.data : null;
}

const postDataSetValuesToDestination = async (dataSetValues , orgunit, week) => {

    await dhis2dest.post('/dataValueSets?importStrategy=CREATE_AND_UPDATE', JSON.stringify(dataSetValues)).catch((error) => {
       logNOTOK(orgunit, week)
    })
}

const getDataSetCompletenessFromSource =async (orgunit , week) => {
    return await dhis2source.get(`/completeDataSetRegistrations?orgUnit=${orgunit}&dataSet=${dataset}&period=${week}`).catch((error) => {
        return null;
    });
}

const postDataSetCompletenessToDestination = async (completeness) => {
    await dhis2dest.post('/completeDataSetRegistrations?importStrategy=CREATE_AND_UPDATE', JSON.stringify(completeness)).catch((error) => {
        console.log(error);
    })
}

const logOK = (week) =>{
    const date = new Date();
    fs.writeFile(`./logs/logs_${date.getFullYear()}${date.getMonth()}${date.getDate()}_OK.log`, `${week}\n`, { flag: 'a+' }, err => { })
}

const logNOTOK = (orgunit , week) =>{
    const date = new Date();
    fs.writeFile(`./logs/logs_${date.getFullYear()}${date.getMonth()}${date.getDate()}_not_OK.log`, `${orgunit} , ${week} \n`, { flag: 'a+' }, err => { });
    console.log(error);
}


run();


// cron.schedule("0 0 18 * * *", async () => await run());