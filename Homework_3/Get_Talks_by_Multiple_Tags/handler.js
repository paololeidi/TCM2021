const connect_to_db = require('./db');

const talk = require('./Talk');

module.exports.get_talks_by_multiple_tags = (event, context, callback) => {
    context.callbackWaitsForEmptyEventLoop = false;
    console.log('Received event:', JSON.stringify(event, null, 2));
    let body = {}
    if (event.body) {
        body = JSON.parse(event.body)
    }
    // set default
    if(!body._tags) {
        callback(null, {
                    statusCode: 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not fetch the talks. Tags is null.'
        })
    }
    
    if (!body.doc_per_page) {
        body.doc_per_page = 6
    }
    if (!body.page) {
        body.page = 1
    }
    
    if(!body.must_match) {
        body.must_match = 3
    }
    
    
    connect_to_db().then(() => {
        console.log('=> get_all talks');
        talk.find({tags: {$all: body._tags}})
            .skip((body.doc_per_page * body.page) - body.doc_per_page)
            .limit(body.doc_per_page)
            .then(talks => {
                    if(talks.length < body.doc_per_page && body.must_match < body.doc_per_page) {
                        let slicedTags = body._tags.slice(0, body.must_match)
                        talk.find({tags: {$all: slicedTags}})
                            .limit(body.doc_per_page - talks.length)
                            .then(adjustedTalks => {
                                talks = talks.concat(adjustedTalks)
                                callback(null, {
                                    statusCode: 200,
                                    body: JSON.stringify(talks)
                                })
                            })
                    } else {
                        callback(null, {
                            statusCode: 200,
                            body: JSON.stringify(talks)
                        })
                    }
                }
            )
            .catch(err =>
                callback(null, {
                    statusCode: err.statusCode || 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not fetch the talks.'
                })
            );
    });
};
