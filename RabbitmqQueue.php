<?php

namespace yii\rabbitmq;

use PhpAmqpLib\Wire\AMQPTable;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Connection\AMQPStreamConnection;

class RabbitmqQueue
{
    public $accessId;
    public $accessKey;
    public $endPoint;
    public $topicEndPoint;
    public $port;
    public $vhost = '/';

    private $client;
    private $channel;
    private $topicClient;

    /**
     * 队列client初始化
     * @param $end_point, $port, $access_id, $access_key, $vhost
     */
    public function __construct($end_point, $port, $access_id, $access_key, $vhost)
    {
        $this->endPoint = $end_point;
        $this->port = $port;
        $this->accessId = $access_id;
        $this->accessKey = $access_key;
        $this->vhost = $vhost;

        $this->client = new AMQPStreamConnection($this->endPoint, $this->port, $this->accessId, $this->accessKey, $this->vhost);
    }

    /**
     * 获取队列client
     * @param bool $reset 是否重置
     * @return Client
     */
    protected function getClient($reset = false)
    {
        if (!$this->client || $reset) {
            $this->client = new AMQPStreamConnection($this->endPoint, $this->port, $this->accessId, $this->accessKey, $this->vhost);
        }
        return $this->client;
    }

    /**
     * 获取队列channel
     * @return mixed 返回 channel 对象
     */
    protected function getChannel()
    {
        if (empty($this->channel) || null === $this->channel->getChannelId()) {
            $this->channel = $this->getClient()->channel();
        }
        return $this->channel;
    }

    /**
     * @param $exchange 交换机名称
     * @param string $exchange_type 交换机类型 可选 direct fanout  topic headers x-delayed-message（延时队列）
     * @param bool $passive 是否被动申明,true的话会检测是否存在相同的名称的交换机
     * @param bool $durable 是否持久化
     * @param bool $auto_delete 是否自动删除
     * @return array
     */
    public function createExchange($exchange, $exchange_type = 'fanout', $passive = false, $durable = false, $auto_delete = false, $internal = false, $nowait = false, $arguments = array())
    {
        if (!empty($arguments)) {
            $arguments = new AMQPTable($arguments);
        }
        try {
            $result = $this->getChannel()->exchange_declare($exchange, $exchange_type, $passive, $durable, $auto_delete, $internal, $nowait, $arguments);
            return self::returnOut(0, '', ['exchange' => $exchange, 'exchange_type' => $exchange_type]);
        } catch (\Exception $e) {
            return self::returnOut($e->getCode(), $e->getMessage());
        }
    }

    /**
     * 删除交换机
     * @param $exchange 交换机名称
     * @param bool $if_unused 是否使用
     * @param bool $nowait
     * @return array 返回结果
     */
    public function exchangeDelete($exchange, $if_unused = false, $nowait = false)
    {
        try {
            $this->getChannel()->exchange_delete($exchange, $if_unused, $nowait);
            return self::returnOut(0, '', ['exchange' => $exchange]);
        } catch (\Exception $e) {
            return self::returnOut($e->getCode(), $e->getMessage());
        }
    }

    public function exchangeBind($destination, $source, $routing_key = '', $nowait = false, $arguments = array())
    {
        if (!empty($arguments)) {
            $arguments = new AMQPTable($arguments);
        }
        try {
            $this->getChannel()->exchange_bind($destination, $source, $routing_key, $nowait, $arguments);
            return self::returnOut(0, '', ['destination' => $destination, 'source' => $source]);
        } catch (\Exception $e) {
            return self::returnOut($e->getCode(), $e->getMessage());
        }
    }

    public function exchangeUnBind($destination, $source, $routing_key = '', $nowait = false, $arguments = array())
    {
        if (!empty($arguments)) {
            $arguments = new AMQPTable($arguments);
        }
        try {
            $this->getChannel()->exchange_unbind($destination, $source, $routing_key, $nowait, $arguments);
            return self::returnOut(0, '', ['destination' => $destination, 'source' => $source]);
        } catch (\Exception $e) {
            return self::returnOut($e->getCode(), $e->getMessage());
        }
    }


    /**
     * 创建队列
     * @param $name 队列名称
     * @param array $attributes 队列参数
     * @param bool $passive 是否被动申明
     * @param bool $durable 是否持久化
     * @param bool $exclusive 是否排外
     * @param bool $autoDelete 是否自动删除
     * @param bool $nowait
     * @return array 返回创建结果
     */
    public function createQueue($name, $attributes = [], $passive = false, $durable = true, $exclusive = false, $autoDelete = false, $nowait = false)
    {
        try {
            if (!empty($attributes)) {
                $attributes = new AMQPTable($attributes);
            }
            $result = $this->getChannel()->queue_declare($name, $passive, $durable, $exclusive, $autoDelete, $nowait, $attributes);
            return self::returnOut(0, '', ['name' => $name, 'requestId' => null, 'queueId' => null]);
        } catch (\Exception $e) {
            return self::returnOut($e->getCode(), $e->getMessage());
        }
    }

    /**
     * 绑定队列
     * @param $queue 队列名称
     * @param $exchange 交换机名称
     * @param string $routing_key 路由key
     * @param bool $nowait 是否等待返回
     * @param array $arguments 绑定参数
     * @return array 返回操作结果
     */
    public function queueBind($queue, $exchange, $routing_key = '', $nowait = false, $arguments = array())
    {
        if (!empty($arguments)) {
            $arguments = new AMQPTable($arguments);
        }
        try {
            $this->getChannel()->queue_bind($queue, $exchange, $routing_key, $nowait, $arguments);
            return self::returnOut(0, '', ['queue' => $queue, 'exchange' => $exchange]);
        } catch (\Exception $e) {
            return self::returnOut($e->getCode(), $e->getMessage());
        }
    }

    /**
     * 接触队列绑定
     * @param $queue 队列名
     * @param $exchange 交换机名
     * @param string $routing_key 路由key
     * @param array $arguments 参数
     * @return array 返回结果
     */
    public function queueUnBind($queue, $exchange, $routing_key = '', $arguments = [])
    {
        if (!empty($arguments)) {
            $arguments = new AMQPTable($arguments);
        }
        try {
            $this->getChannel()->queue_unbind($queue, $exchange, $routing_key, $arguments);
            return self::returnOut(0, '', ['queue' => $queue, 'exchange' => $exchange, 'routing_key' => $routing_key]);
        } catch (\Exception $e) {
            return self::returnOut($e->getCode(), $e->getMessage());
        }
    }

    /**
     * 获取队列信息
     * @param string $name 队列名 all为全部返回
     * @return array 返回队列信息
     */
    public function getQueue($name = 'all')
    {
        $queueList = [];
        $api = 'http://' . $this->endPoint . ":15672/api/queues";
        try {
            $result = $this->http($api, 'GET', [], ['username' => $this->accessId, 'password' => $this->accessKey]);
            if (!empty($result)) {
                foreach ($result as $k => $v) {
                    if ($name != 'all') {
                        if ($name == $v['name']) {
                            $queueList[] = [
                                'name' => $v['name'],
                                'vhost' => $v['vhost'],
                                'exclusive' => $v['exclusive'],
                                'auto_delete' => $v['auto_delete'],
                                'durable' => $v['durable'],
                                'info' => $v['garbage_collection']
                            ];
                        }
                    } else {
                        $queueList[] = [
                            'name' => $v['name'],
                            'vhost' => $v['vhost'],
                            'exclusive' => $v['exclusive'],
                            'auto_delete' => $v['auto_delete'],
                            'durable' => $v['durable'],
                            'info' => $v['garbage_collection']
                        ];
                    }
                }
            }
            return self::returnOut(0, '', ['queueData' => $queueList]);
        } catch (\Exception $e) {
            return self::returnOut($e->getCode(), $e->getMessage());
        }
    }

    /**
     * 删除队列
     * @param $name 队列名
     * @return array 返回数据
     */
    public function deleteQueue($name)
    {
        try {
            $result = $this->getChannel()->queue_delete($name);
            return self::returnOut(0, '', ['name' => $name]);
        } catch (\Exception $e) {
            return self::returnOut($e->getCode(), $e->getMessage());
        }
    }

    /**
     * 发送消息
     * @param $queue 队列名称
     * @param $body 内容
     * @param int $delaySeconds 可设置延时时间，单位毫秒
     * @param array $properties 发送信息属性
     * @param null $exchange 交换机名称
     * @param null $routing_key 路由key
     * @return array 返回结果
     */
    public function sendMessage($queue, $body, $delaySeconds = 0, $properties = [], $exchange = null, $routing_key = null)
    {
        try {
            $message = new AMQPMessage($body, $properties);
            if ($delaySeconds) {
                $headers = ['x-delay' => $delaySeconds];
                $headers = new AMQPTable($headers);
                $message->set('application_headers', $headers);
            }
            if (empty($routing_key) && !empty($queue)) $routing_key = $queue;
            $this->getChannel()->basic_publish($message, $exchange, $routing_key);
            return self::returnOut(0, '', ['messageBodyMD5' => '', 'messageId' => '']);
        } catch (\Exception $e) {
            return self::returnOut($e->getCode(), $e->getMessage());
        }
    }

    /**
     * 批量发送信息
     * @param $queue 队列名
     * @param $bodys 内容数组
     * @param int $delaySeconds 可设置延时时间，单位毫秒
     * @param array $properties 发送信息属性
     * @param null $exchange 交换机名称
     * @param null $routing_key 路由key
     * @return array 返回结果
     */
    public function batchSendMessage($queue, $bodys, $delaySeconds = 0, $properties = [], $exchange = null, $routing_key = null)
    {
        try {
            if (!is_array($bodys) || empty($bodys)) {
                throw new \Exception('message is empty');
            }
            foreach ($bodys as $k => $v) {
                $message = new AMQPMessage($v, $properties);
                if ($delaySeconds) {
                    $headers = ['x-delay' => $delaySeconds];
                    $headers = new AMQPTable($headers);
                    $message->set('application_headers', $headers);
                }
                if (empty($routing_key) && !empty($queue)) $routing_key = $queue;
                $this->getChannel()->batch_basic_publish($message, $exchange, $routing_key);
            }
            $this->getChannel()->publish_batch();
            return self::returnOut(0, '', ['num' => count($bodys)]);
        } catch (\Exception $e) {
            return self::returnOut($e->getCode(), $e->getMessage());
        }
    }

    /**
     * 获取队列信息
     * @param $queue 队列名
     * @param int $waitSeconds 等待时间,此参数忽略
     * @return array 返回结果
     */
    public function receiveMessage($queue, $waitSeconds = 10)
    {
        $receipt = [];
        try {
            $message = $this->getChannel()->basic_get($queue);
            if (!empty($message)) {
                $receipt = [
                    'body' => $message->body,
                    'body_size' => $message->body_size,
                    'is_truncated' => $message->is_truncated,
                    'content_encoding' => $message->content_encoding,
                    'delivery_info' => $message->delivery_info,
                ];
            }
            return self::returnOut(0, '', $receipt);
        } catch (\Exception $e) {
            return self::returnOut($e->getCode(), $e->getMessage());
        }
    }

    /**
     * 批量获取队列消息
     * @param $queue 队列名称
     * @param int $num 获取条数
     * @param int $waitSeconds 等待秒数，此参数忽略
     * @return array 返回结果
     */
    public function batchReceiveMessage($queue, $num = 10, $waitSeconds = 30)
    {
        $receipt = [];
        try {
            $i = 0;
            while ($i < $num) {
                $message = $this->getChannel()->basic_get($queue);
                if (empty($message)) {
                    break;
                } else {
                    $receipt[] = [
                        'body' => $message->body,
                        'body_size' => $message->body_size,
                        'is_truncated' => $message->is_truncated,
                        'content_encoding' => $message->content_encoding,
                        'delivery_info' => $message->delivery_info,
                    ];
                }
                $i++;
            }
            return self::returnOut(0, '', $receipt);
        } catch (\Exception $e) {
            return self::returnOut($e->getCode(), $e->getMessage());
        }
    }

    /**
     * 消费或者删除消息
     * @param $queue 队列名称
     * @param $receiptHandle delivery_tag值
     * @return array 返回操作结果
     */
    public function deleteMessage($queue, $receiptHandle)
    {
        try {
            $this->getChannel()->basic_ack($receiptHandle);
            return self::returnOut(0, '', ['delivery_tag' => $receiptHandle]);
        } catch (\Exception $e) {
            return self::returnOut($e->getCode(), $e->getMessage());
        }
    }

    /**
     * 批量消费或者删除消息
     * @param $queue 队列名称
     * @param $receiptHandles delivery_tag值
     * @return array 返回操作结果
     */
    public function batchDeleteMessage($queue, $receiptHandles)
    {
        try {
            if (!empty($receiptHandles)) {
                foreach ($receiptHandles as $k => $v) {
                    $this->getChannel()->basic_ack($v);
                }
            }
            return self::returnOut(0, '', ['delivery_tags' => $receiptHandles]);
        } catch (\Exception $e) {
            return self::returnOut($e->getCode(), $e->getMessage());
        }
    }

    /**
     * 标准化输出值
     * @param $code 信息码
     * @param string $message 信息描述
     * @param array $data 输出值
     * @return array 返回输出数组
     */
    private static function returnOut($code, $message = '', $data = [])
    {
        return ['code' => $code, 'message' => $message, 'data' => $data];
    }

    /**
     * 设置属性默认值
     * @param $attributes 属性数组
     * @param $name 属性名
     * @param null $value 默认值
     * @return null 返回值
     */
    private static function getAttributes($attributes, $name, $value = null)
    {
        $result = $value;
        if (isset($attributes[$name])) {
            $result = $attributes[$name];
        }
        return $result;
    }

    /**
     * 对象转数组
     * @param $obj 需要转换的对象
     * @return array|void 转换后的数组
     */
    private static function objectToArray($obj)
    {
        $obj = (array)$obj;
        foreach ($obj as $k => $v) {
            if (gettype($v) == 'resource') {
                return;
            }
            if (gettype($v) == 'object' || gettype($v) == 'array') {
                $obj[$k] = (array)self::objectToArray($v);
            }
        }
        return $obj;
    }

    /**
     * http请求
     * @param $url 请求地址
     * @param $method 请求方法
     * @param $params 请求参数
     * @param $auth 是否验证
     * @return mixed 返回结果
     */
    private function http($url, $method, $params, $auth)
    {
        $curl = curl_init();
        curl_setopt($curl, CURLOPT_URL, $url);
        curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($curl, CURLOPT_SSL_VERIFYPEER, false);
        curl_setopt($curl, CURLOPT_SSL_VERIFYHOST, false);
        $header[] = "Content-Type:application/json;charset=utf-8";
        if (!empty($header)) {
            curl_setopt($curl, CURLOPT_HTTPHEADER, $header);
        }
        $timeout = 30;
        curl_setopt($curl, CURLOPT_CONNECTTIMEOUT, $timeout);
        switch ($method) {
            case "GET":
                curl_setopt($curl, CURLOPT_HTTPGET, true);
                break;
            case "POST":
                if (is_array($params)) {
                    $params = json_encode($params, 320);
                }
                curl_setopt($curl, CURLOPT_CUSTOMREQUEST, "POST");
                curl_setopt($curl, CURLOPT_POSTFIELDS, $params);
                break;
            case "PUT":
                curl_setopt($curl, CURLOPT_CUSTOMREQUEST, "PUT");
                curl_setopt($curl, CURLOPT_POSTFIELDS, json_encode($params, 320));
                break;
            case "DELETE":
                curl_setopt($curl, CURLOPT_CUSTOMREQUEST, "DELETE");
                curl_setopt($curl, CURLOPT_POSTFIELDS, $params);
                break;
        }
        if (!empty($auth) && isset($auth['username']) && isset($auth['password'])) {
            curl_setopt($curl, CURLOPT_USERPWD, "{$auth['username']}:{$auth['password']}");
        }
        $data = curl_exec($curl);
        $status = curl_getinfo($curl, CURLINFO_HTTP_CODE);
        curl_close($curl);
        $res = json_decode($data, true);
        return $res;
    }

    /**
     * 结构函数 销毁chanel
     */
    public function __destruct()
    {
        if ($this->channel) {
            try {
                $this->channel->close();
            } catch (\Exception $e) {
                //忽略
            }
        }
        if ($this->client && $this->client->isConnected()) {
            try {
                $this->client->close();
            } catch (\Exception $e) {
                //忽略
            }
        }
    }


}

?>