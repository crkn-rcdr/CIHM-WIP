package CIHM::WIP::App::Export;

use common::sense;
use MooseX::App::Command;
use CIHM::WIP;
use File::Spec;
use JSON;
use Cwd qw(realpath);
use File::Path qw(make_path);
use Data::Dumper;


extends qw(CIHM::WIP::App);

parameter 'cmd' => (
  is => 'rw',
  isa => 'Str',
  required => 1,
  documentation => q[Sub-command is one of: aip,sip,metadata],
);

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

option 'stage' => (
  is => 'rw',
  isa => 'Str',
  documentation => q[The destination stage (Example: Processing)],
);

option 'configid' => (
  is => 'rw',
  isa => 'Str',
  documentation => q[The configuration ID (Example: heritage)],
);

option 'destdir' => (
  is => 'rw',
  isa => 'Str',
  documentation => q[Destination directory in WIP (when not using --stage,--configid)],
);

command_short_description 'Exports information from the repository';
command_usage 'wip export <aip|sip|metadata> [<uid> ...] [long options...]';

sub run {
    my ($self) = @_;

    if (! (($self->cmd) =~ /(aip|sip|metadata)/)) {
        die "Invalid sub-command: " . $self->cmd . "\n";
    }
    
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

    if($self->stage) {
        if (!($self->stage) || !($self->configid)) {
            die "--stage and --configid should be used togther\n";
        }
        if ($self->depositor) {
            die "--depositor shouldn't be used when using --stage and --configid\n";
        }
        if ($self->destdir) {
            die "--destdir shouldn't be used when using --stage and --configid\n";
        }
        my $stage=$self->WIP->findstagei($self->stage) ||
            die "Stage '".$self->stage."' didn't match any configured stage";
        $self->stage($stage);

    } elsif ($self->destdir) {
        my $path = realpath($self->destdir);
        if (!$path || !(-d $path)) {
            die "--destdir=".$self->destdir." is not a valid path\n";
        }
        my ($rsync,$relpath) = $self->WIP->find_rsync($path);
        if (!$relpath) {
            die "--destdir=".$self->destdir." not within WIP directory\n";
        }
        $self->destdir($relpath);
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
    }

    my $res = $wipmeta->post("/".$wipmeta->database."/_all_docs?include_docs=true", { keys => \@aipids }, {deserializer => 'application/json'});

    if (!($res->data) || !($res->data->{rows})) {
        warn "_all_docs GET for <tdrepo> return code: " . $res->code . "\n";
    }
    foreach my $row (@{$res->data->{rows}}) {
        next if ! exists $row->{doc};
        my $aipid=$row->{doc}->{'_id'};
        $aipinfo->{$aipid}->{wipdb}=1;
        if (exists $row->{doc}->{filesystem}) {
            my $fs=$row->{doc}->{filesystem};
            next if (!$fs->{stage} || $fs->{stage} eq '');
            $aipinfo->{$aipid}->{fs}=
                $self->WIP->stages->{$fs->{stage}}."/".$fs->{configid}."/".$fs->{identifier};
        }
    }

    # Might as well know if these AIPs exist, and can be exported...
    $res = $tdrepo->post("/".$tdrepo->database."/_design/tdr/_view/newestaip?group=true", { keys => \@aipids }, {deserializer => 'application/json'});
    if (!($res->data) || !($res->data->{rows})) {
        warn "_view/newestaip for <tdrepo> GET return code: " . $res->code . "\n";
    }
    foreach my $row (@{$res->data->{rows}}) {
        $aipinfo->{$row->{key}}->{'date'}=$row->{value}[0];
        $aipinfo->{$row->{key}}->{'repos'}=$row->{value}[1];
    }


    foreach my $aip (@aipids) {
        if (exists $aipinfo->{$aip}->{'date'}) {
            my $exportreq = {
                request => 'export',
                type =>$self->cmd
            };
            my $dest;
            if ($self->destdir) {
                $exportreq->{wipDir}=$self->destdir;
            } elsif ($self->stage) {
                $exportreq->{fs}->{stage}=$self->stage;
                $exportreq->{fs}->{configid}=$self->configid;
                $exportreq->{fs}->{identifier}=$aipinfo->{$aip}->{id};
            } elsif (exists $aipinfo->{$aip}->{fs}) {
                $exportreq->{fs}->{existing}=JSON::true;
            } else {
                warn "Don't know where to store $aip -- skipping\n";
                next;
            }
            $self->WIP->wipmeta->update_basic(
                $aip, 
                {
                    processreq => encode_json($exportreq)
                });
        } else {
            warn "$aip not found in TDR\n";
        }
    }
}

sub WIP {
    my $self = shift;
    return $self->{WIP};
}

1;
