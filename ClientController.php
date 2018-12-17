<?php

namespace yii\rabbitmq;

class ClientController
{
    /**
     * 信息发布
     * @param $body 内容
     * @return array 返回结果
     */
    public function publishAction()
    {
        $body = 'String Type';
        $queue_name = 'exchange.example';
        $rabbitmq_server = new RabbitmqQueue('127.0.0.1', '5672',  'admin',  'admin', '/');
        $resp = $rabbitmq_server->sendMessage(
            $queue_name,
            $body,
            0,
            [],
            'exchange'
        );
        return $resp;
    }
}