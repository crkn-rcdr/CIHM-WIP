package CIHM::WIP::App::Ingest;

use common::sense;
use Data::Dumper;
use MooseX::App::Command;
use Try::Tiny;
use CIHM::WIP;
use Cwd qw(realpath);
use JSON;

extends qw(CIHM::WIP::App);

option 'update' => (
  is => 'rw',
  isa => 'Bool',
  documentation => q[Allow update of an existing AIP],
);

parameter 'changelog' => (
  is => 'rw',
  isa => 'Str',
  required => 1,
  documentation => q[Text for changelog],
);

parameter 'depositor' => (
  is => 'rw',
  isa => 'Str',
  required => 1,
  documentation => q[The depositor for the SIP],
);

parameter 'sip_root' => (
  is => 'rw',
  isa => 'Str',
  required => 1,
  documentation => q[Path to the SIP to be ingested into AIP],
);

option 'premove' => (
  is => 'rw',
  isa => 'Str',
  documentation => q[Also add a 'move' request to the specified stage prior to doing other processing.],
);

option 'postmove' => (
  is => 'rw',
  isa => 'Str',
  documentation => q[Also add a 'move' request to the specified stage if successful.],
);

command_short_description 'Submit SIP to be ingested into an AIP (update or new)';

sub run {
  my ($self) = @_;

  my $WIP = CIHM::WIP->new($self->conf);

  if (!$WIP->depositor_valid($self->depositor)) {
      $self->error($self->depositor." is not a valid depositor");
  }


  my $prestage;
  if ($self->premove) {
      $prestage=$WIP->findstagei($self->premove) ||
          die "Stage '".$self->premove."' didn't match any configured stage";
  }
  my $poststage;
  if ($self->postmove) {
      $poststage=$WIP->findstagei($self->postmove) ||
          die "Stage '".$self->postmove."' didn't match any configured stage";
  }

  if (!$WIP->tdrepo) {
      $self->error("<tdrepo> configuration missing");
  }

  my $realroot=realpath($self->sip_root);
  if (!$realroot) {
      $self->error($self->sip_root." doesn't exist");
  }
  if (! -d $realroot) {
      $self->error("$realroot is not a directory");
  }
  my $metadata = $realroot."/data/metadata.xml";
  if (! -f $metadata) {
      $self->error("Metadata file not found: $metadata\n");
  }
  my ($rsyncurl,$path) = $WIP->find_rsync($realroot);
  if (! $rsyncurl)  {
      $self->error("$realroot not within <paths> in configuration");
  }

  my $objid;
  try {
      $objid = $WIP->parse_mets($metadata);
  } catch {
      $self->error("parse_mets error: ". $_);
  };

  my $uid=$self->depositor.".$objid";
  my $type;
  if ($self->update) {
      if (! $WIP->check_aip_exists($uid)) {
          $self->error("--update requested, but AIP missing from <tdrepo> database for $uid found at $realroot");
      }
      $type="update";
  } else {
      if ($WIP->check_aip_exists($self->depositor.".".$objid)) {
          $self->error("--update not requested, but AIP exists in <tdrepo> database for $uid found at $realroot");
      }
      $type="new";
  }
  $self->log->info("Submitting $realroot uid=$uid type=$type");

  if ($prestage) {
      $WIP->wipmeta->update_basic(
          $uid, 
          { 
              nocreate => JSON::true,
              processreq => encode_json(
                  {
                      request => 'move',
                      stage => $prestage,
                  })
          });
  }
  $WIP->wipmeta->update_basic(
      $uid,
      { 
          nocreate => JSON::true,
          processreq => encode_json(
              {
                  request => 'ingest',
                  type => $type,
                  changelog => $self->changelog,
                  rsyncurl => $rsyncurl.$path,
              })

      });
  if ($poststage) {
      $WIP->wipmeta->update_basic(
          $uid, 
          { 
              nocreate => JSON::true,
              processreq => encode_json(
                  {
                      request => 'move',
                      stage => $poststage,
                  })
          });
  }
}

sub error {
  my ($self,$message) = @_;

  my $cleanmessage = $message;
  $cleanmessage =~ s/\n/\\n/mg;
  $self->log->error($cleanmessage);

  die "ERROR: $message\n";
}

1;
