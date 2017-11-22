package CIHM::WIP::App::Mv;

use common::sense;
use MooseX::App::Command;
use CIHM::WIP;
use File::Spec;
use JSON;
use Cwd;
use File::Path qw(make_path);

extends qw(CIHM::WIP::App);

parameter 'stage' => (
  is => 'rw',
  isa => 'Str',
  required => 1,
  documentation => q[The destination stage (Example: Processing)],
);
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
option 'fromhere' => (
  is => 'rw',
  isa => 'Bool',
  documentation => q[Directories matching names of identifiers should be moved from current working directory],
);
option 'empty' => (
  is => 'rw',
  isa => 'Bool',
  documentation => q[Empty directories should be created (useful for series AIPs)],
);
option 'request' => (
  is => 'rw',
  isa => 'Bool',
  documentation => q[Generate a Processing Request to add to queue, rather than move immediately],
);
option 'updateid' => (
  is => 'rw',
  isa => 'Bool',
  documentation => q[Used with --request to update the identifier field],
);
option 'updateconf' => (
  is => 'rw',
  isa => 'Bool',
  documentation => q[Used with --request to update the ConfigID],
);

command_usage 'wip mv <stage> <configid> <identifier> [<identifier> ...] [long options...]';

command_short_description 'Moves file data directories in WIP';


sub run {
  my ($self) = @_;

  $self->{WIP} = CIHM::WIP->new($self->conf);
  my $configdocs=$self->WIP->configdocs ||
      die "Can't retrieve configuration documents\n";

  my $wipmeta=$self->WIP->wipmeta ||
      die "<wipmeta> access to <wipmeta> database not configured\n";

  my $myconfig=$configdocs->{$self->configid} ||
      die $self->configid." is not a valid configuration id\n";

  my $depositor=$myconfig->{depositor} ||
      die "Depositor not set for ".$self->configid."\n";

  my $stage=$self->WIP->findstagei($self->stage) ||
      die "Stage '".$self->stage."' didn't match any configured stage";

  my $cwd=getcwd;

  my @identifiers = @{($self->extra_argv)[0]};
  unshift (@identifiers,$self->identifier);

  my @aipids;
  my $aipinfo={};
  foreach my $identifier (@identifiers) {
      my $objid=$self->WIP->i2objid($identifier,$self->configid);
      if ($self->WIP->objid_valid($objid)) {
          push @aipids, "$depositor.$objid";
          $aipinfo->{"$depositor.$objid"}->{identifier}=$identifier;
      } else {
          warn "$objid not valid OBJID\n";
      }
  }

  my $res = $wipmeta->post("/".$wipmeta->database."/_all_docs?include_docs=true", { keys => \@aipids }, {deserializer => 'application/json'});

  if (!($res->data) || !($res->data->{rows})) {
      warn "_all_docs GET return code: " . $res->code . "\n";
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

  foreach my $aip (@aipids) {
      my $identifier=$aipinfo->{$aip}->{identifier};
      if ($self->request) {
          my $request= {
              request => 'move',
              stage => $stage,
              empty => $self->empty ? JSON::true : JSON::false
          };
          if ($self->updateid || $self->empty) {
              $request->{identifier}=$identifier;
          };
          if ($self->updateconf || $self->empty) {
              $request->{configid}=$self->configid;
          };
          my $retdata = $self->WIP->wipmeta->update_basic(
              $aip,
              { 
                  nocreate => JSON::true,
                  processreq => encode_json($request)
              });
          if ($retdata ne 'update') {
              warn "Move processing request for $aip returned $retdata\n";
          }
      } else {
          my $destparent = $self->WIP->stages->{$stage}."/".$self->configid; 
          my $destdir = "$destparent/$identifier";
          my $sourcedir;

          if ($self->fromhere)  {
              $sourcedir="$cwd/$identifier";
          } elsif (defined $aipinfo->{$aip}->{fs}) {
              $sourcedir=$aipinfo->{$aip}->{fs};
          } elsif (!$self->empty) {
              warn "$aip($identifier): Don't know where to move from...\n";
              next;
          }
          if (!$self->empty && ! -d $sourcedir) {
              warn "$aip($identifier): Source $sourcedir not directory.\n";
              next;
          }
          if (!$self->empty && $sourcedir eq $destdir) {
              warn "$aip($identifier): Only notifying DB\n";
          } else {
              if (-e $destdir) {
                  warn "$aip($identifier): Destination $destdir already exists.\n";
                  next;
              } else {
                  # warn "Would rename $sourcedir to $destdir\n";
                  if (! -d $destparent) {
                      make_path($destparent) || die "$aip($identifier): Could not make path $destparent: $!\n";
                  }
                  if ($self->empty) {
                      mkdir $destdir 
                          or die "$aip($identifier): Could not create $destdir: $!\n";
                  } else {
                      rename $sourcedir,$destdir 
                          or die "$aip($identifier): Could not rename $sourcedir to $destdir: $!\n";
                  }
              }
          }

          # Notify DB
          my $retdata = $self->WIP->wipmeta->update_filesystem(
              $aip,
              {
                  "filesystem" => encode_json({
                      stage => $stage,
                      configid => $self->configid,
                      identifier => $identifier
                                              })
              });
          if (!$retdata) {
              warn "$aip($identifier): update_filesystem didn't return data\n";
          }
      }
  }
}
sub WIP {
    my $self = shift;
    return $self->{WIP};
}

1;
