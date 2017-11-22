package CIHM::WIP::App::Buildsip;

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

option 'validate' => (
  is => 'rw',
  isa => 'Bool',
  documentation => q[Validate the built SIP],
);

option 'premove' => (
  is => 'rw',
  isa => 'Str',
  default => 'Processing',
  documentation => q[Also add a 'move' request to the specified stage prior to doing other processing.],
);
option 'nopremove' => (
  is => 'rw',
  isa => 'Bool',
  documentation => q[Disable the --premove request.],
);
option 'empty' => (
  is => 'rw',
  isa => 'Bool',
  documentation => q[Empty directories should be created (useful for series AIPs) as part of the --premove],
);

option 'postmove' => (
  is => 'rw',
  isa => 'Str',
  default => 'Trashcan',
  documentation => q[Also add a 'move' request to the specified stage if successful.],
);
option 'nopostmove' => (
  is => 'rw',
  isa => 'Bool',
  documentation => q[Disable adding the --postmove request.],
);

option 'ingest' => (
  is => 'rw',
  isa => 'Str',
  documentation => q[Also add an ingest request of specified type ("new" or "update")],
);

option 'changelog' => (
  is => 'rw',
  isa => 'Str',
  documentation => q[Text for changelog. Mandatory when --ingest= specified.],
);


command_short_description 'Exports information from the repository';
command_usage 'wip buildsip <uid> [<uid> ...] [long options...]';

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
    if ($self->ingest) {
        if (!$self->changelog) {
            die "--changelog is mandatory when --ingest used\n";
        }
        if ($self->ingest ne "new" && $self->ingest ne "update") {
            die "--ingest= must be one of 'new' or 'update'\n";
        }
    }
    if ($self->empty && ! ($self->configid)) {
        die "--configid= required if --empty used\n";
    }
    my $prestage;
    if (!$self->nopremove) {
        $prestage=$self->WIP->findstagei($self->premove) ||
            die "Stage '".$self->premove."' didn't match any configured stage";
    }
    my $poststage;
    if (!$self->nopostmove) {
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
        if (!$depositor) {
            warn "Depositor undefined for $id\n";
            next;
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
            next;
        }
        if ($prestage) {
            my $request={
                request => 'move',
                stage => $prestage,
                empty => $self->empty ? JSON::true : JSON::false
            };
            if ($self->empty && $self->configid) {
                $request->{configid}=$self->configid;
                $request->{identifier}=$identifier;
            }
            $self->WIP->wipmeta->update_basic(
                "$depositor.$objid", 
                { 
                    nocreate => JSON::true,
                    processreq => encode_json($request)
                });
        }

        $self->WIP->wipmeta->update_basic(
            "$depositor.$objid", 
            { 
                nocreate => JSON::true,
                processreq => encode_json(
                    {
                        request => 'buildsip',
                        validate => $self->validate ? JSON::true : JSON::false,
                    })
            });

        if ($self->ingest) {
            $self->WIP->wipmeta->update_basic(
                "$depositor.$objid", 
                { 
                    nocreate => JSON::true,
                    processreq => encode_json(
                        {
                            request => 'ingest',
                            type => $self->ingest,
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
