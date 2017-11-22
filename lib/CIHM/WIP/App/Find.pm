package CIHM::WIP::App::Find;

use common::sense;
use Data::Dumper;
use MooseX::App::Command;
use CIHM::WIP;
use File::Spec;
use JSON;

extends qw(CIHM::WIP::App);

parameter 'configid' => (
  is => 'rw',
  isa => 'Str',
  required => 1,
  documentation => q[The configuration ID (Example: heritage)],
);
parameter 'identifier' => (
  is => 'rw',
  isa => 'Str',
  required => 1,
  documentation => q[The identifier being looked up (Example: C-11)],
);

option 'notdr' => (
  is => 'rw',
  isa => 'Bool',
  documentation => q[Don't look up if AIP in TDR],
);
option 'nowip' => (
  is => 'rw',
  isa => 'Bool',
  documentation => q[Don't look up if AIP related data directory in WIP],
);
option 'noid' => (
  is => 'rw',
  isa => 'Bool',
  documentation => q[Don't display identifier in output],
);
option 'csv' => (
  is => 'rw',
  isa => 'Bool',
  documentation => q[Output in a format useful as input to programs],
);
command_usage 'wip find <configid> <identifier> [<identifier> ...] [long options...]';

command_short_description 'Finds AIP in WIP and TDR';


sub run {
  my ($self) = @_;

  $self->{WIP} = CIHM::WIP->new($self->conf);
  my $configdocs=$self->WIP->configdocs ||
      die "Can't retrieve configuration documents\n";
  my $myconfig=$configdocs->{$self->configid} ||
      die $self->configid." is not a valid configuration id\n";
  my $depositor=$myconfig->{depositor} ||
      die "Depositor not set for ".$self->configid."\n";

  my @identifiers = @{($self->extra_argv)[0]};
  unshift (@identifiers,$self->identifier);


  my @aipids;
  my $idinfo={};
  foreach my $identifier (@identifiers) {
      my $objid=$self->WIP->i2objid($identifier,$self->configid);
      if ($self->WIP->objid_valid($objid)) {
          push @aipids, "$depositor.$objid";
          $idinfo->{"$depositor.$objid"}=$identifier;
      } else {
          warn "$objid not valid OBJID\n";
      }
  }

  my $wipinfo={};
  if (!($self->nowip) && $self->WIP->wipmeta) {
      my $wipmeta=$self->WIP->wipmeta;

      my $res = $wipmeta->post("/".$wipmeta->database."/_all_docs?include_docs=true", { keys => \@aipids }, {deserializer => 'application/json'});

      if (!($res->data) || !($res->data->{rows})) {
          warn "_all_docs GET return code: " . $res->code . "\n";
      }
      foreach my $row (@{$res->data->{rows}}) {
          next if ! exists $row->{doc};
          $wipinfo->{$row->{doc}->{'_id'}}->{db}=1;
          if (exists $row->{doc}->{filesystem}) {
              my $fs=$row->{doc}->{filesystem};
              next if (!$fs->{stage} || $fs->{stage} eq '');
              $wipinfo->{$row->{doc}->{'_id'}}->{fs}=
                  $fs->{stage}."/".$fs->{configid}."/".$fs->{identifier};
          }
      }
  }
#  print Dumper($wipinfo);

  my $tdrinfo={};
  if (!($self->notdr) && $self->WIP->tdrepo) {
      my $tdrepo=$self->WIP->tdrepo;

      my $res = $tdrepo->post("/".$tdrepo->database."/_design/tdr/_view/newestaip?group=true", { keys => \@aipids }, {deserializer => 'application/json'});

      if (!($res->data) || !($res->data->{rows})) {
          warn "_all_docs GET return code: " . $res->code . "\n";
      }
      foreach my $row (@{$res->data->{rows}}) {
          $tdrinfo->{$row->{key}}->{'date'}=$row->{value}[0];
          $tdrinfo->{$row->{key}}->{'repos'}=$row->{value}[1];
      }
  }
#  print Dumper($tdrinfo);


  foreach my $aip (@aipids) {
      if ($self->csv) {
          my @csvline;

          if (!($self->noid)) {
              push (@csvline,$aip);
          }
          if (!($self->nowip)) {
              my $indoc="false";
              my $path="";
              if ($wipinfo->{$aip}) {
                  $indoc="true";
                  if ($wipinfo->{$aip}->{fs}) {
                      $path=$wipinfo->{$aip}->{fs};
                  }
              }
              push (@csvline,$indoc,$path);
          }
          if (!($self->notdr)) {
              my $date="";
              my $repos="";
              if ($tdrinfo->{$aip}) {
                  $date=$tdrinfo->{$aip}->{date};
                  $repos='"'.join(",",@{$tdrinfo->{$aip}->{repos}}).'"';
              }
              push (@csvline,$date,$repos);
          }
          if (@csvline) {
              print join (",",@csvline)."\n";
          }
      } else {
          if (!($self->noid)) {
              print "$aip:";
          }
          if (!($self->nowip)) {
              if ($wipinfo->{$aip}) {
                  print " in 'wipmeta'";
                  if ($wipinfo->{$aip}->{fs}) {
                      print "(".$wipinfo->{$aip}->{fs}.")";
                  }
              } else {
                  print " not in 'wipmeta'";
              }
          }
          if (!($self->notdr)) {
              if ($tdrinfo->{$aip}) {
                  print " in 'tdrepo' with manifest date ".$tdrinfo->{$aip}->{date}." in repositories ".join(",",@{$tdrinfo->{$aip}->{repos}});
              } else {
                  print " not in 'tdrepo'";
              }

          }
          print "\n";
      }
  }
}
sub WIP {
    my $self = shift;
    return $self->{WIP};
}

1;
