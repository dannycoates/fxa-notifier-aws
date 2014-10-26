var AWS = require('aws-sdk')
var inherits = require('util').inherits
var EventEmitter = require('events').EventEmitter

function Source(snsTopicArn) {
  this.topic = snsTopicArn
  // Pull the region info out of the topic arn.
  // For some reason we need to pass this in explicitly.
  // Format is "arn:aws:sns:<region>:<other junk>"
  this.sns = new AWS.SNS({ region: this.topic.split(':')[3] })
  EventEmitter.call(this)
}
inherits(Source, EventEmitter)

Source.prototype.send = function (event, callback) {
  var msg = event.data
  msg.event = event.name
  this.sns.publish(
    {
      TopicArn: this.topic,
      Message: JSON.stringify(msg)
    },
    function (err) {
      if (callback) {
        callback(err)
      }
      else if (err) {
        this.emit('error', err)
      }
    }.bind(this)
  )
}

module.exports = Source
