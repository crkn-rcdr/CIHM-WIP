package CIHM::WIP::Ingest::Worker;

use strict;
use AnyEvent;
use Try::Tiny;
use Config::General;
use CIHM::TDR;
use CIHM::TDR::Swift;
use CIHM::TDR::Repository;
use CIHM::TDR::REST::wipmeta;
use CIHM::TDR::REST::tdrepo;
use CIHM::TDR::ContentServer;
use CIHM::WIP::Ingest::Process;
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

    if (! ($self->{tdr} = CIHM::TDR->new($configpath))) {
        die "Wasn't able to build CIHM::TDR object\n";
    }
    if (! ($self->{repo} = $self->{tdr}->repo)) {
        die "Wasn't able to build Repository object\n";
    }

    Log::Log4perl->init_once("/etc/canadiana/tdr/log4perl.conf");
    $self->{logger} = Log::Log4perl::get_logger("CIHM::TDR");

    my %confighash = new Config::General(
        -ConfigFile => $configpath,
        )->getall;

    if (! $self->repo->tdrepo) {
        die "Missing <tdrepo> configuration\n";
    }

    # So far we only need a few options (existance checked earlier)
    $self->{tempdir} = $confighash{ingest}{tempdir};
    if (exists $confighash{stages} &&
        ref($confighash{stages}) eq "HASH") {
        $self->{stages} = $confighash{stages};
    } else {
        die "Missing <stages> configuration\n"
    }

    # Undefined if no <wipmeta> config block
    if (exists $confighash{wipmeta}) {
        $self->{wipmeta} = new CIHM::TDR::REST::wipmeta (
            server => $confighash{wipmeta}{server},
            database => $confighash{wipmeta}{database},
            type   => 'application/json',
            conf   => $configpath,
            clientattrs => {timeout => 3600},
            );
    } else {
        die "Missing <wipmeta> configuration block in config\n";
    }

    $self->{cserver} = new CIHM::TDR::ContentServer($configpath);
    if (!$self->{cserver}) {
        die "Missing ContentServer configuration.\n";
    }

    $self->{swift} = new CIHM::TDR::Swift({
	configpath => $configpath
					  });
    if (!$self->{swift}) {
        die "Missing Swift configuration.\n";
    }
}


# Simple accessors for now -- Do I want to Moo?
sub log {
    my $self = shift;
    return $self->{logger};
}
sub config {
    my $self = shift;
    return $self->{config};
}
sub wipmeta {
    my $self = shift;
    return $self->{wipmeta};
}
sub tdr {
    my $self = shift;
    return $self->{tdr};
}
sub swift {
    my $self = shift;
    return $self->{swift};
}
sub repo {
    my $self = shift;
    return $self->{repo};
}
sub hostname {
    my $self = shift;
    return $self->{hostname};
}
sub tempdir {
    my $self = shift;
    return $self->{tempdir};
}
sub stages {
    my $self = shift;
    return $self->{stages};
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


sub ingest {
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
          request => 'ingest',
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
          my $process = new  CIHM::WIP::Ingest::Process(
              {
                  aip => $aip,
                  configpath => $configpath,
                  log => $self->log,
                  tdr => $self->tdr,
                  wipmeta => $self->wipmeta,
		  swift => $self->swift,
                  repo => $self->repo,
                  hostname => $self->hostname,
                  tempdir => $self->tempdir,
                  stages => $self->stages
              });
          $processdoc = $process->process;
      } catch {
          $status = JSON::false;
          $self->log->error("$aip: $_");
          $self->{message} .= "Caught: " . $_;
      };
      $processdoc->{status}=$status;
      $processdoc->{message}=$self->{message};
      $processdoc->{request}='ingest';
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
