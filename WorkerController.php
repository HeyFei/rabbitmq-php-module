<?php

namespace yii\rabbitmq;

class WorkerController extends AbstractCommonJob
{
    const QUEUE_NAME = 'exchange.example';

    public function processMsg($body) : bool
    {
        if(empty($body)) {
            return false;
        }
    }

    public function setQueueName()
    {
        $this->queue_name = self::QUEUE_NAME;
    }
}