package CIHM::WIP::App::UpdateMetadata;

use common::sense;
use Data::Dumper;
use MooseX::App::Command;
use Try::Tiny;
use CIHM::WIP;
use Cwd qw(realpath);
use File::Slurp;
use JSON;

extends qw(CIHM::WIP::App);

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
  documentation => q[The depositor for the AIP],
);

parameter 'file' => (
  is => 'rw',
  isa => 'Str',
  required => 1,
  documentation => q[Filename of the metadata .xml file],
);



command_short_description 'Submit metadata to update existing AIP';

sub run {
  my ($self) = @_;

  my $WIP = CIHM::WIP->new($self->conf);

  if (!$WIP->depositor_valid($self->depositor)) {
      $self->error($self->depositor." is not a valid depositor");
  }

  my $metadata = $self->file;
  if (! -f $metadata) {
      $self->error("Metadata file not found: $metadata");
  }
  my $objid;
  try {
      $objid = $WIP->parse_mets($metadata);
  } catch {
      $self->error("parse_mets error: ". $_);
  };

  my $uid = $self->depositor.".".$objid;

  my $mdata;
  try {
      $mdata = read_file($metadata);
  } catch {
      $self->error("Error reading $metadata: ". $_);
  };

  $self->log->info("Submitting $metadata as update to $uid");

  $WIP->wipmeta->put_attachment($uid, {
      content => $mdata,
      filename => "metadata.xml",
      type => "application/xml",
      updatedoc => {
          processreq => encode_json({
              request => 'ingest',
              type => "metadata",
              changelog => $self->changelog,
                                    })
      }
                                });
}

sub error {
  my ($self,$message) = @_;

  my $cleanmessage = $message;
  $cleanmessage =~ s/\n/\\n/mg;
  $self->log->error($cleanmessage);

  die "ERROR: $message\n";
}

1;
