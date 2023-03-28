<?php

declare(strict_types=1);

namespace Jenky\Atlas\Pool;

use Amp;
use Amp\Future;

final class AmpPool implements PoolInterface
{
    /**
     * @var array<array-key, \Amp\Future>
     */
    private array $requests = [];

    public function queue($key, callable $request): void
    {
        $rq = $request instanceof \Closure
            ? $request
            : \Closure::fromCallable($request);

        $this->requests[$key] = Amp\async($rq);
    }

    public function send(): array
    {
        return Future\awaitAll($this->requests)[1] ?? [];
    }
}
