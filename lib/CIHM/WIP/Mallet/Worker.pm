package CIHM::WIP::Mallet::Worker;

use strict;
use AnyEvent;
use Try::Tiny;
use CIHM::WIP;
use Config::General;
use CIHM::Swift::Client;
use CIHM::WIP::Mallet::Process;
use Log::Log4perl;
use JSON;
use Data::Dumper;
use Net::Domain qw(hostname hostfqdn hostdomain domainname);

our $self;

sub initworker {
    my $configpath = shift;
    our $self;

    AE::log debug => "Initworker ($$): $configpath";

    $self = bless {};

    $self->{hostname} = hostfqdn();

    if (! ($self->{WIP} = CIHM::WIP->new($configpath))) {
        die "Wasn't able to build CIHM::WIP object\n";
    }

    my %confighash = new Config::General(
        -ConfigFile => $configpath,
        )->getall;

    # Undefined if no <swift> config block
    if(exists $confighash{swift}) {
        my %swiftopt = (
            furl_options => { timeout => 120 }
            );
        foreach ("server","user","password","account", "furl_options") {
            if (exists  $confighash{swift}{$_}) {
                $swiftopt{$_}=$confighash{swift}{$_};
            }
        }
        $self->{swift}=CIHM::Swift::Client->new(%swiftopt);
        $self->{container}=$confighash{swift}{container};
    } else {
        die "No <swift> configuration block in ".$configpath."\n";
    }

    Log::Log4perl->init_once("/etc/canadiana/wip/log4perl.conf");
    $self->{logger} = Log::Log4perl::get_logger("CIHM::WIP::Mallet");

}


# Simple accessors for now -- Do I want to Moo?
sub log {
    my $self = shift;
    return $self->{logger};
}
sub WIP {
    my $self = shift;
    return $self->{WIP};
}
sub swift {
    my $self = shift;
    return $self->{swift};
}
sub container {
    my $self = shift;
    return $self->{container};
}
sub wipmeta {
    my $self = shift;
    return $self->WIP->wipmeta;
}
sub hostname {
    my $self = shift;
    return $self->{hostname};
}

sub warnings {
    my $warning = shift;
    our $self;
    my $aip="unknown";

    if ($self) {
        $self->{message} .= $warning;
        $aip = $self->{aip};
    }
    # Strip wide characters before  trying to log
    $warning =~ s/[^\x00-\x7f]//g;
    $self->log->warn($aip.": $warning");
}


sub swing {
  my ($aip,$date,$configpath) = @_;
  our $self;

  # Capture warnings
  local $SIG{__WARN__} = sub { &warnings };

  if (!$self) {
      initworker($configpath);
  }
  $self->{aip}=$aip;
  $self->{message}='';

  AE::log debug => "$aip Before ($$)";

  # Accept Job, but also check if someone else already accepted it
  my $res = $self->wipmeta->update_basic($aip,{ 
      "processing" => encode_json({
          request => 'mallet',
          reqdate => $date,
          host => $self->hostname
                                  })
                                         });
  if (!$res) {
      $self->log->error("no result from attempt to set processhost");
      return ($aip);
  } elsif ($res eq 'no processReq') {
      $self->log->info("$aip: already completed by other host");
      return ($aip);
  } elsif ($res eq 'already set') {
      $self->log->info("$aip: Process already accepted by this host");
      return ($aip);
  } elsif ($res eq 'other host') {
      $self->log->info("$aip: Process accepted by other host");
      return ($aip);
  } elsif ($res eq 'update') {

      my $processdoc = {};
      my $status;

      # Handle and record any errors
      try {
          $status = JSON::true;
          my $process = new  CIHM::WIP::Mallet::Process(
              {
                  aip => $aip,
                  configpath => $configpath,
                  hostname => $self->hostname,
                  WIP => $self->WIP,
                  swift => $self->swift,
		  swiftcontainer => $self->container,
                  log => $self->log,
              });
          $processdoc = $process->process;
      } catch {
          $status = JSON::false;
          $self->log->error("$aip: $_");
          $self->{message} .= "Caught: " . $_;
      };
      $processdoc->{status}=$status;
      $processdoc->{message}=$self->{message};
      $processdoc->{request}='mallet';
      $processdoc->{reqdate}=$date;
      $processdoc->{host}=$self->hostname;
      $self->postResults($aip,$processdoc);
  } else {
      $self->log->error("$aip: acceptJob returned $res");
  }

  AE::log debug => "$aip After ($$)";

  return ($aip);
}

sub postResults {
    my ($self,$aip,$processdoc) = @_;

    $self->wipmeta->update_basic($aip,{ 
        "processed" => encode_json($processdoc)
    });
}

1;
