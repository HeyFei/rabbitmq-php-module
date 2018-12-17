<?php

use yii\rabbitmq\RabbitmqQueue;

class Test
{
    protected $queue_name = '';
    protected $total_msg_count = 10;

    const QUEUE_NAME = 'exchange.example';

    /**
     * 信息发布
     * @param  string $body 内容
     * @return array 返回结果
     */
    public function actionPublish()
    {
        $body = 'String Type';
        $rabbitmq_server = new RabbitmqQueue('127.0.0.1', '5672',  'admin',  'admin', '/');
        $resp = $rabbitmq_server->sendMessage(
            $this->queue_name,
            $body,
            0,
            [],
            'exchange'
        );
        return $resp;
    }

    /**
     * 消费信息
     * @return string 返回结果
     */
    public function actionConsume()
    {
        $rabbitmq_server = new RabbitmqQueue('127.0.0.1', '5672',  'admin',  'admin', '/');
        $i = 0;
        while (true) {
            usleep(1000*10);
            if ($i >= $this->total_msg_count) {
                break;
            }
            $i++;
            $message = $rabbitmq_server->receiveMessage($this->queue_name);
            if (0 === $message['code']) {
                $body = $message['data']['body'] ?? '';
            } else {
                $body = '';
            }

            if (empty($body)) {
                continue;
            }
            $receipt_handle = $message['data']['delivery_info']['delivery_tag'];
            $rabbitmq_server->deleteMessage($this->queue_name, $receipt_handle);
        }
    }
}