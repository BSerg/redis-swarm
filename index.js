import redis from 'redis';
import uuid from 'uuid';

class Req {
    constructor(initial) {
        this.scope = initial.scope;
        this.groupChannel = initial.scope;
        this.data = initial.data;
        this._id = _id || uuid.v4();
        this.createdAt = createdAt || new Date();
    }

    toJSON() {
        return {data: this.data, _id: this._id, createdAt: this.createdAt.getTime()};
    }

    static fromJSON(requestJson) {
        let req = JSON.parse(requestJson);
        return new Req(req.data, req._id, new Date(req.createdAt))
    }
}

const defaultOpts = {
    scope: 'swarm',
    channel: 'foo',
    instanceId: uuid.v4(),
    redis: {
        host: 'localhost',
        port: 6379,
    }
}

export default class SwarmClient {
    constructor(opts) {
        this.opts = {...defaultOpts, ...opts};

        this.scope = this.opts.scope;
        this.channel = this.opts.channel;
        this.instanceId = this.opts.instanceId;

        this.__callbacks = {};
        this.__cancels = {};

        this.swarm = [];
        this.__swarmIndex = {};

        this.init();
    }

    _updateSwarm(data) {
        data.date = new Date(data.date);
        const channel = `${data.scope}:${data.channel}:${data.instanceId}`;
        if (this.__swarmIndex[channel]) {
            let index = this.swarm.indexOf(this.__swarmIndex[channel]);
            if (index == -1) {
                this.swarm.push(data);
            } else {
                this.swarm[index] = data;
            }
        } else {
            this.swarm.push(data);
        }
        this.__swarmIndex[channel] = data;
    }

    init() {
        this.sub = redis.createClient(this.opts.redis);
        this.pub = this.sub.duplicate();
        
        this.sub.on('message', (channel, msg) => {
            let _data = JSON.parse(msg);
            switch (channel) {

                case this.scope + ':heartbeat':
                    if (_data.instanceId !== this.instanceId) {
                        this._updateSwarm(_data);
                    }
                    break;
                
                case this.scope + ':' + this.group:
                case this.scope + ':' + this.group + ':' + this.instanceId:
                    this.__callbacks[channel] && this.__callbacks[channel](msg);
                    break;
            }
            
        });

        this.sub.subscribe(this.scope + ':heartbeat');

        this.__heartBeatProcess = setInterval(() => {
            this.heartBeat();
        }, 1000);

        this.__checkAliveProcess = setInterval(() => {
            this.swarm.forEach((instance, index) => {
                let _date = new Date();
                _date.setSeconds(_date.getSeconds() - 5);
                instance.isActive = instance.date >= _date;
            });
        }, 3000);
    }

    close() {
        clearInterval(this.__heartBeatProcess);
        clearInterval(this.__checkAliveProcess);
    }

    heartBeat() {
        let data = {
            scope: this.opts.scope, 
            group: this.opts.group, 
            instanceId: this.opts.instanceId,
            date: new Date(),
        };
        this.pub.publish(this.scope + ':heartbeat', JSON.stringify(data));
    }

    listen(channel, callback = () => {}) {
        this.__callbacks[channel] = callback;
        this.sub.subscribe(channel);
    }

    removeListener(channel) {
        delete this.__callbacks[channel];
        this.sub.unsubscribe(channel);
    }

    call(channel, msg, _id) {
        return new Promise((resolve, reject) => {
            _id = _id || uuid.v4();
            this.__callbacks[_id] = response => {
                delete this.__callbacks[_id];
                delete this.__cancels[_id];
                resolve(response);
            };
            this.__cancels[_id] = () => {
                delete this.__callbacks[_id];
                delete this.__cancels[_id];
                reject({isCanceled: true});
            };
            this.sub.subscribe(_id);
            this.pub.publish(channel, JSON.stringify({msg, _id}));
        });
    }

    callOneOf(group, msg, _id) {
        let instances = this.swarm.filter(instance => {
            return instance.group == group && instance.isActive;
        });
        if (instances.length == 0) {
            return Promise.reject('No one instance found');
        } else {
            let randomInstance = instances[Math.floor(Math.random() * instances.length)];
            let channel = instance.scope + ':' + instance.group + ':' + instance.instanceId;
            return this.call(channel, msg, _id);
        }
    }

    cancel(_id) {
        this.__cancels[_id] && this.__cancels[_id]();
    }

    getSwarm() {
        return this.swarm;
    }
}