package CIHM::WIP::App;

use MooseX::App;
use Log::Log4perl;
with 'MooseX::Log::Log4perl';

BEGIN {
  Log::Log4perl->init_once("/etc/canadiana/wip/log4perl.conf");
}

option 'conf' => (
  is => 'rw',
  isa => 'Str',
  default => "/etc/canadiana/wip/wip.conf",
  documentation => q[An option that specifies where you can find a config file if not default],
);

1;
