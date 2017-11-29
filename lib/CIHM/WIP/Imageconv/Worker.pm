package CIHM::WIP::Imageconv::Worker;

use strict;
use AnyEvent;
use Try::Tiny;
use CIHM::WIP;
use CIHM::WIP::Imageconv::Process;
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
    $self->{logger} = Log::Log4perl::get_logger("CIHM::WIP::Imageconv");

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


sub imageconv {
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
          request => 'imageconv',
          reqdate => $date,
          host => $self->hostname
                                  })
                                         });
  if (!$res) {
      $self->log->error("no result from attempt to set to imageconv");
      return ($aip);
  } elsif ($res eq 'no processReq') {
      $self->log->info("$aip: no imageconv request -- two instances running?");
      return ($aip);
  } elsif ($res ne 'update') {
      $self->log->error("$aip: acceptJob returned $res");
      return ($aip);
  }

  my $status;
  # Handle and record any errors
  try {
      $status = JSON::true;
      new  CIHM::WIP::Imageconv::Process(
          {
              aip => $aip,
              configpath => $configpath,
              WIP => $self->WIP,
              log => $self->log,
              hostname => $self->hostname
          })->process;
  } catch {
      $status = JSON::false;
      $self->log->error("$aip: $_");
      $self->{message} .= "Caught: " . $_;
  };
  my $processdoc = {};
  $processdoc->{status}=$status;
  $processdoc->{message}=$self->{message};
  $processdoc->{request}='imageconv';
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
