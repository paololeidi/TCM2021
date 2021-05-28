const mongoose = require('mongoose');

const talk_schema = new mongoose.Schema({
    _id: String,
    watch_next_videos: Array
}, { collection: 'tedx_video_data' });

module.exports = mongoose.model('talk', talk_schema);
