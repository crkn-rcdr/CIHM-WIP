package CIHM::WIP::Export::Worker;

use strict;
use AnyEvent;
use Try::Tiny;
use CIHM::WIP;
use CIHM::WIP::Export::Process;
use CIHM::TDR::Swift;
use JSON;
use Data::Dumper;
use Log::Log4perl;
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

    Log::Log4perl->init_once("/etc/canadiana/wip/log4perl.conf");
    $self->{logger} = Log::Log4perl::get_logger("CIHM::WIP::Export");

    $self->{swift} = new CIHM::TDR::Swift({
	configpath => $configpath
					  });
    if (!$self->swift) {
        die "Missing Swift configuration in $configpath.\n";
    }
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
sub wipmeta {
    my $self = shift;
    return $self->WIP->wipmeta;
}
sub swift {
    my $self = shift;
    return $self->{swift};
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


sub export {
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

  # Accept Job
  my $res = $self->wipmeta->update_basic($aip,{ 
      "processing" => encode_json({
          request => 'export',
          reqdate => $date,
          host => $self->hostname
                                  })
                                         });
  if (!$res) {
      $self->log->error("no result from attempt to set to exporting");
      return ($aip);
  } elsif ($res eq 'no exportReq') {
      $self->log->info("$aip: no export request -- two instances running?");
      return ($aip);
  } elsif ($res ne 'update') {
      $self->log->error("$aip: acceptJob returned $res");
      return ($aip);
  }

  my $processdoc = {};
  my $status;

  # Handle and record any errors
  try {
      $status = JSON::true;
      my $process = new  CIHM::WIP::Export::Process(
          {
              aip => $aip,
              configpath => $configpath,
              WIP => $self->WIP,
              log => $self->log,
              swift => $self->swift,
          });
      $processdoc = $process->export;
  } catch {
      $status = JSON::false;
      $self->log->error("$aip: $_");
      $self->{message} .= "Caught: " . $_;
  };
  $processdoc->{status}=$status;
  $processdoc->{message}=$self->{message};
  $processdoc->{request}='export';
  $processdoc->{reqdate}=$date;
  $processdoc->{host}=$self->hostname;
  $self->postResults($aip,$processdoc);

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
