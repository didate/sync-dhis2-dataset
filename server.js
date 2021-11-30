const { dhis2source, dhis2dest } = require('./dhis2');
const fs = require('fs');
const csv = require('csv-parser');
const moment = require('moment');  



const getLastFewsWeek = (numberOfWeek=4) =>{
    index =1 ;
    const weeks = [];
    while(index <= numberOfWeek){
        const period = moment().add(-(index++), 'w');
        weeks.push(`${period.year()}W${period.isoWeek()}`);
    }
    return weeks;
}


const run = async () => {

    
    const periods = getLastFewsWeek();

    const dataset = 'AhWR8jm7KQW'; // SAP
    const orgunits = [];
    const date = new Date();

    fs.createReadStream('./orgunit.csv').pipe(csv()).on('data', (row) => {
        // ADD row to datas array
        orgunits.push(row);
    }).on('end', async () => {
        console.log('Debut du traitement .....');

        for (let i = 0; i < periods.length; i++) { // boucle sur les semaines
                const currentWeek = periods[i];

            for (let index = 0; index < orgunits.length; index++) {
                const orgunit = orgunits[index];

                // lire les donnees d'une semaine a partir de dhis 2 source
                try {
                    const datavaluesfromdhis = await dhis2source.get(`/dataValueSets?orgUnit=${orgunit.uid}&dataSet=${dataset}&period=${currentWeek}`).catch((error) => {
                        console.log(error);
                        fs.writeFile(`./logs/logs_${date.getFullYear()}${date.getMonth()}${date.getDate()}_not_OK.log`, `${orgunit.uid} , ${currentWeek} \n`, { flag: 'a+' }, err => { })
                        return;
                    });

                    let dataSetValues = datavaluesfromdhis ? datavaluesfromdhis.data : null;
                    if (dataSetValues && dataSetValues.dataValues && dataSetValues.dataValues.length > 0) {
                        //delete dataSetValues['completeDate']
                        // Envoi des donnees vers le dhis 2 destination
                        await dhis2dest.post('/dataValueSets?importStrategy=CREATE_AND_UPDATE', JSON.stringify(dataSetValues)).catch((error) => {
                            console.log(error);
                            fs.writeFile(`./logs/logs_${date.getFullYear()}${date.getMonth()}${date.getDate()}_not_OK.log`, `${orgunit.uid} , ${currentWeek} \n`, { flag: 'a+' }, err => { })
                        })

                        // recuperation du completeness

                        const completeness = await dhis2source.get(`/completeDataSetRegistrations?orgUnit=${orgunit.uid}&dataSet=${dataset}&period=${currentWeek}`).catch((error) => {
                            console.log(error);
                            return;
                        });

                    if (completeness && completeness.data ) {
                        const completenessData =completeness.data;

                        console.log(completenessData)

                        await dhis2dest.post('/completeDataSetRegistrations?importStrategy=CREATE_AND_UPDATE', JSON.stringify(completenessData)).catch((error) => {
                            console.log(error);
                        })
                    }

                    }
                } catch (error) {
                    console.log(error)
                    fs.writeFile(`./logs/logs_${date.getFullYear()}${date.getMonth()}${date.getDate()}_not_OK.log`, `${orgunit.uid} , ${currentWeek} \n`, { flag: 'a+' }, err => { })
                }
            }
            fs.writeFile(`./logs/logs_${date.getFullYear()}${date.getMonth()}${date.getDate()}_OK.log`, `${currentWeek}\n`, { flag: 'a+' }, err => { })
        }

        console.log('Fin du traitement .....');
    });
}

run();