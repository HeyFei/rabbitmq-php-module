<?php
namespace yii\rabbitmq;

use yii\console\Controller;

abstract class AbstractCommonJob extends Controller
{
    protected $queue_name = '';
    protected $total_msg_count = 1000;
    public function actionIndex()
    {
        $this->setQueueName();
        $this->launch();
    }
    private function getQueueName()
    {
        return $this->queue_name;
    }
    public function launch()
    {
        $rabbitmq_server = new RabbitmqQueue($end_point = '', $port = '', $access_id = '', $access_key = '', $vhost = '');
        $i = 0;
        while (true) {
            usleep(1000*10);
            if ($i >= $this->total_msg_count) {
                break;
            }
            $i++;
            $message = $rabbitmq_server->receiveMessage($this->getQueueName());
            if (0 === $message['code']) {
                $body = $message['data']['body'] ?? '';
            } else {
                $body = '';
            }
            $bool_rep = $this->processMsg($body);
            if (!$bool_rep) {
                continue;
            }
            $receipt_handle = $message['data']['delivery_info']['delivery_tag'];
            $rabbitmq_server->deleteMessage($this->queue_name, $receipt_handle);
        }
    }
    public abstract function processMsg($message) : bool;
    public abstract function setQueueName();
}