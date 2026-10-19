/**
 * Unified Redis Store Client
 * Supports both standard TCP Redis (via ioredis) and Upstash REST API.
 * 100% dynamic: configured entirely via environment variables.
 */

const Redis = require('ioredis');

class DynamicStoreClient {
  constructor(options = {}) {
    this.upstashUrl = options.upstashUrl || process.env.UPSTASH_REDIS_REST_URL || null;
    this.upstashToken = options.upstashToken || process.env.UPSTASH_REDIS_REST_TOKEN || null;
    this.redisUrl = options.redisUrl || process.env.REDIS_URL || 'redis://127.0.0.1:6379';
    this.keyPrefix = options.keyPrefix || process.env.REDIS_KEY_PREFIX || '';

    this.isUpstash = Boolean(this.upstashUrl && this.upstashToken);
    this.tcpClient = null;

    if (!this.isUpstash) {
      this.tcpClient = new Redis(this.redisUrl, {
        keyPrefix: this.keyPrefix,
        maxRetriesPerRequest: 3,
        retryStrategy: (times) => {
          const delay = Math.min(times * 100, 2000);
          return delay;
        },
        lazyConnect: false,
      });

      this.tcpClient.on('error', (err) => {
        // Emit error but do not crash unless unhandled
        if (process.env.DEBUG_STORE === 'true') {
          console.error('[Store:Redis] Connection error:', err.message);
        }
      });
    }
  }

  _formatKey(key) {
    if (this.isUpstash && this.keyPrefix) {
      return `${this.keyPrefix}${key}`;
    }
    return key;
  }

  async _upstashRequest(command, ...args) {
    const cleanUrl = this.upstashUrl.replace(/\/+$/, '');
    const endpoint = [cleanUrl, command, ...args.map(encodeURIComponent)].join('/');
    
    const response = await fetch(endpoint, {
      method: 'GET',
      headers: {
        Authorization: `Bearer ${this.upstashToken}`,
        Accept: 'application/json',
      },
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Upstash REST error [${response.status}]: ${errorText}`);
    }

    const json = await response.json();
    return json.result;
  }

  async get(key) {
    const formattedKey = this._formatKey(key);
    if (this.isUpstash) {
      return await this._upstashRequest('get', formattedKey);
    }
    return await this.tcpClient.get(formattedKey);
  }

  async set(key, value, { ttlSec = null, nx = false } = {}) {
    const formattedKey = this._formatKey(key);
    const serializedValue = typeof value === 'object' ? JSON.stringify(value) : String(value);

    if (this.isUpstash) {
      const args = [formattedKey, serializedValue];
      if (ttlSec) {
        args.push('ex', String(ttlSec));
      }
      if (nx) {
        args.push('nx');
      }
      return await this._upstashRequest('set', ...args);
    }

    if (ttlSec && nx) {
      return await this.tcpClient.set(formattedKey, serializedValue, 'EX', ttlSec, 'NX');
    }
    if (ttlSec) {
      return await this.tcpClient.set(formattedKey, serializedValue, 'EX', ttlSec);
    }
    if (nx) {
      return await this.tcpClient.set(formattedKey, serializedValue, 'NX');
    }
    return await this.tcpClient.set(formattedKey, serializedValue);
  }

  async del(key) {
    const formattedKey = this._formatKey(key);
    if (this.isUpstash) {
      return await this._upstashRequest('del', formattedKey);
    }
    return await this.tcpClient.del(formattedKey);
  }

  async ping() {
    if (this.isUpstash) {
      return await this._upstashRequest('ping');
    }
    return await this.tcpClient.ping();
  }

  async close() {
    if (this.tcpClient) {
      try {
        await this.tcpClient.quit();
      } catch {
        this.tcpClient.disconnect();
      }
    }
  }
}

module.exports = { DynamicStoreClient };
