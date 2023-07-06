<?php

declare(strict_types=1);

namespace Jenky\Atlas\Pool\React;

use Clue\React\Mq\Queue;
use Jenky\Atlas\Contracts\ConnectorInterface;
use Jenky\Atlas\Pool\Exceptions\UnsupportedException;
use Jenky\Atlas\Pool\PoolInterface;
use Jenky\Atlas\Pool\PoolTrait;
use Jenky\Atlas\Request;
use Jenky\Atlas\Response;
use React\Async;
use React\Http\Browser;
use React\Promise;

final class Pool implements PoolInterface
{
    use PoolTrait;

    private ConnectorInterface $connector;

    public function __construct(ConnectorInterface $connector)
    {
        if ($connector->client() instanceof Client) {
            $this->connector = clone $connector;
        } elseif (method_exists($connector, 'withClient')) {
            $this->connector = $connector->withClient(new Client(new Browser()));
        } else {
            throw new UnsupportedException('The client is not supported.');
        }
    }

    public function send(iterable $requests): array
    {
        if (\PHP_VERSION_ID >= 80200) {
            // Temporary solution until https://github.com/clue/reactphp-mq/pull/36 release
            error_reporting(E_ALL & ~E_DEPRECATED & ~E_USER_DEPRECATED);
        }

        $queue = new Queue($this->concurrency, null, fn ($cb) => Async\async($cb)());

        $promises = static function (ConnectorInterface $connector) use ($requests, $queue) {
            foreach ($requests as $key => $request) {
                if ($request instanceof Request) {
                    yield $key => static fn () => $queue(static fn (): Response => $connector->send($request));
                } elseif (is_callable($request)) {
                    yield $key => static fn () => $queue(static fn (): Response => $request($connector));
                } else {
                    throw new \InvalidArgumentException('Each value of the iterator must be a Jenky\Atlas\Request or a \Closure that returns a Jenky\Atlas\Response object.');
                }
            }
        };

        return Async\await(Promise\all(Async\parallel($promises($this->connector)))); //@phpstan-ignore-line
    }
}
