package rmqkit

import amqp "github.com/rabbitmq/amqp091-go"

func (r *RabbitMQ) setupTopology(ch *amqp.Channel) error {
	c := r.cfg

	if r.typ == ConsumerOnly {
		_, err := ch.QueueDeclarePassive(c.Queue, c.Durable, false, false, false, nil)

		return err
	}

	if c.Exchange != "" {
		if err := ch.ExchangeDeclare(c.Exchange, c.ExchangeType, c.Durable, false, false, false, nil); err != nil {
			return err
		}
	}

	if _, err := ch.QueueDeclare(c.Queue, c.Durable, false, false, false, nil); err != nil {
		return err
	}

	if c.Exchange == "" {
		return nil
	}

	return ch.QueueBind(c.Queue, c.RoutingKey, c.Exchange, false, nil)
}
