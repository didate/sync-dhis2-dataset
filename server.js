const { dhis2, query } = require('./dhis2');
const fs = require('fs');
const csv = require('csv-parser');




const run = async () => {

    // En cas de reprise, supprimer les lignes des annees deja traiter et recuperer la derniere semaine traité dans le fichier des OK
    const periods = [
        { year: 2017, wStart: 1, wEnd: 52 },
        { year: 2018, wStart: 1, wEnd: 52 },
        { year: 2019, wStart: 1, wEnd: 52 },
        { year: 2020, wStart: 1, wEnd: 53 }
    ]

    const dataset = 'AhWR8jm7KQW'; // AhWR8jm7KQW = SAP
    const orgunits = [];
    const date = new Date();

    fs.createReadStream('./orgunit.csv').pipe(csv()).on('data', (row) => {
        // ADD row to datas array
        orgunits.push(row);
    }).on('end', async () => {
        console.log('Debut du traitement .....');

        for (let i = 0; i < periods.length; i++) { // boucle sur les annees
            const period = periods[i];
            let debut = period.wStart
            while (debut <= period.wEnd) { // boucle sur les semaines a partir de la semaine de reprise
                const currentWeek = `${period.year}W${debut}`;
                for (let index = 0; index < orgunits.length; index++) {
                    const orgunit = orgunits[index];

                    // lire les donnees d'une semaine a partir de dhis 2 source
                    try {
                        const datavaluesfromdhis = await dhis2.get(`/dataValueSets?orgUnit=${orgunit.uid}&dataSet=${dataset}&period=${currentWeek}`).catch((error) => {
                            console.log(error);
                        });
                        const dataSetValues = datavaluesfromdhis ? datavaluesfromdhis.data : null;
                        if (dataSetValues && dataSetValues.dataValues && dataSetValues.dataValues.length > 0) {
                            // Envoi des donnees vers le dhis 2 destination
                            await query.post('/dataValueSets?importStrategy=CREATE_AND_UPDATE', JSON.stringify(dataSetValues)).catch((error) => {
                                console.log(error);
                                fs.writeFile(`./logs/logs_${date.getFullYear()}${date.getMonth()}${date.getDate()}_not_OK.log`, `${orgunit.uid} , ${currentWeek} \n`, { flag: 'a+' }, err => { })
                            })
                        }
                    } catch (error) {
                        fs.writeFile(`./logs/logs_${date.getFullYear()}${date.getMonth()}${date.getDate()}_not_OK.log`, `${orgunit.uid} , ${currentWeek} \n`, { flag: 'a+' }, err => { })
                    }


                }
                fs.writeFile(`./logs/logs_${date.getFullYear()}${date.getMonth()}${date.getDate()}_OK.log`, `${currentWeek}\n`, { flag: 'a+' }, err => { })

                debut = debut + 1;
            }
        }
    });
}

run();