package CIHM::WIP::App::ManipMetadata;

use common::sense;
use MooseX::App::Command;
use CIHM::WIP;
use File::Spec;
use JSON;
use Cwd qw(realpath);
use File::Path qw(make_path);
use Data::Dumper;


extends qw(CIHM::WIP::App);

parameter 'uid' => (
  is => 'rw',
  isa => 'Str',
  required => 1,
  documentation => q[The uid of the AIP (In contributor.identifier or identifier form)],
);

option 'depositor' => (
  is => 'rw',
  isa => 'Str',
  documentation => q[Depositor to use if not using --configid],
);

option 'configid' => (
  is => 'rw',
  isa => 'Str',
  documentation => q[The configuration ID (Example: heritage)],
);

option 'update' => (
  is => 'rw',
  isa => 'Bool',
  documentation => q[Also add a metadata_update request],
);

option 'changelog' => (
  is => 'rw',
  isa => 'Str',
  documentation => q[Text for changelog. Mandatory when --update specified.],
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

option 'label' => (
  is => 'rw',
  isa => 'Bool',
  documentation => q[Update the item label],
);

option 'clabel' => (
  is => 'rw',
  isa => 'Bool',
  documentation => q[Update the component labels],
);

option 'dmdsec' => (
  is => 'rw',
  isa => 'Bool',
  documentation => q[Update the item dmdSec],
);

option 'cdmdsec' => (
  is => 'rw',
  isa => 'Bool',
  documentation => q[Update the component dmdSec],
);


command_short_description 'Updates specific fields of METS in the main SIP within existing AIP';
command_usage 'wip manip_metadata <uid> [<uid> ...] [long options...]';

sub run {
    my ($self) = @_;

    $self->{WIP} = CIHM::WIP->new($self->conf);
    my $wipmeta=$self->WIP->wipmeta ||
        die "<wipmeta> access to database not configured\n";
    my $tdrepo=$self->WIP->tdrepo ||
        die "<tdrepo> access to database not configured\n";

    if ($self->configid) {
        my $configdocs=$self->WIP->configdocs ||
            die "Can't retrieve configuration documents\n";

        my $myconfig=$configdocs->{$self->configid} ||
            die $self->configid." is not a valid configuration id\n";

        my $depositor=$myconfig->{depositor};
        if ($depositor) {
            $self->depositor($depositor);
        }
    }

    # Check these options early, rather than creating partial requests..
    if ($self->update) {
        if (!$self->changelog) {
            die "--changelog is mandatory when --update used\n";
        }
    }


    my $prestage;
    if ($self->premove) {
        $prestage=$self->WIP->findstagei($self->premove) ||
          die "Stage '".$self->premove."' didn't match any configured stage";
    }
    my $poststage;
    if ($self->postmove) {
        $poststage=$self->WIP->findstagei($self->postmove) ||
            die "Stage '".$self->postmove."' didn't match any configured stage";
    }

    my @identifiers = @{($self->extra_argv)[0]};
    unshift (@identifiers,$self->uid);

    my @aipids;
    my $aipinfo={};
    foreach my $id (@identifiers) {
        my ($depositor,$identifier);
        if (index($id,".") != -1) {
            ($depositor,$identifier) = split(/\./,$id);
        } else {
            $depositor = $self->depositor;
            $identifier = $id;
        }
        if (!$self->WIP->depositor_valid($depositor)) {
            warn "Depositor $depositor for $id not valid\n";
            next;
        }

        my $objid=$identifier;
        if ($self->configid) {
            $objid=$self->WIP->i2objid($identifier,$self->configid);
        }
        if ($self->WIP->objid_valid($objid)) {
            push @aipids, "$depositor.$objid";
            $aipinfo->{"$depositor.$objid"}->{id}=$id;
        } else {
            warn "$objid not valid OBJID\n";
        }
        if ($self->configid) {
            # Make sure that this is set in database
            $self->WIP->wipmeta->update_filesystem(
                "$depositor.$objid",
                {
                    "filesystem" => encode_json({
                        configid => $self->configid,
                        identifier => $identifier
                                                })
                });
        }
        if ($prestage) {
            $self->WIP->wipmeta->update_basic(
                "$depositor.$objid", 
                { 
                    nocreate => JSON::true,
                    processreq => encode_json(
                        {
                            request => 'move',
                            stage => $prestage,
                        })
                });
        }
        $self->WIP->wipmeta->update_basic(
            "$depositor.$objid", 
            { 
                nocreate => JSON::true,
                processreq => encode_json(
                    {
                        request => 'manipmd',
                        label => $self->label ? JSON::true : JSON::false,
                        clabel => $self->clabel ? JSON::true : JSON::false,
                        dmdsec => $self->dmdsec ? JSON::true : JSON::false,
                        cdmdsec => $self->cdmdsec ? JSON::true : JSON::false,
                    })
            });

        if ($self->update) {
            $self->WIP->wipmeta->update_basic(
                "$depositor.$objid", 
                { 
                    nocreate => JSON::true,
                    processreq => encode_json(
                        {
                            request => 'ingest',
                            type => 'metadata',
                            changelog => $self->changelog,
                        })
                });
        }
        if ($poststage) {
            $self->WIP->wipmeta->update_basic(
                "$depositor.$objid", 
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
}

sub WIP {
    my $self = shift;
    return $self->{WIP};
}


1;
