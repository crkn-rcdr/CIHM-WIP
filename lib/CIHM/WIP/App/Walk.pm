package CIHM::WIP::App::Walk;

use common::sense;
use Data::Dumper;
use MooseX::App::Command;
use CIHM::WIP;
use File::Spec;
use JSON;
use Filesys::DfPortable;

use Email::MIME;
use Email::Sender::Simple qw(sendmail);

extends qw(CIHM::WIP::App);

option 'quiet' => (
    is            => 'rw',
    isa           => 'Bool',
    documentation => q[Don't report warnings],
);

option 'report' => (
    is      => 'rw',
    isa     => 'Str',
    default => "",
    documentation =>
q[Comma separate list of email addresses that should receive report. If no @ then standard output],
);

command_short_description
'Walks specific packaging directories to find errors in structure, as well as new or removed directories';

our $self;

sub run {
    our ($self) = @_;

    # Capture warnings
    local $SIG{__WARN__} = sub { &warnings };

    $self->{WIP} = CIHM::WIP->new( $self->conf );
    my $stages = $self->WIP->stages
      || die "Missing <stages> section of config file\n";
    die "Can't retrieve configurationdocuments\n" if ( !$self->configdocs );

    # Hash where keys represent filesystem, value is AIP
    $self->{onfs} = {};

    # Hash where key is AIP, value is CouchDB document
    $self->{aipfs} = {};

    # Set up array to accept warnings, to be sent in nag email.
    $self->{warnings} = [];

    # First walk the filesystem looking for directories, and update DB
    # with current location.
    $self->walk_filesystem();

    # Next walk DB to detect if any previously found directories have been
    # deleted
    $self->walk_dbfilesystem();

    if ( $self->report && $self->report ne '' ) {
        $self->gen_report();
    }
}

sub WIP {
    my $self = shift;
    return $self->{WIP};
}

sub onfs {
    my $self = shift;
    return $self->{onfs};
}

sub aipfs {
    my $self = shift;
    return $self->{aipfs};
}

sub configdocs {
    my $self = shift;
    return $self->WIP->configdocs;
}

sub warnings {
    my $warning = shift;
    our $self;

    # Strip wide characters before  trying to log
    $warning =~ s/[^\x00-\x7f]//g;
    $self->log->warn($warning);

    push @{ $self->{warnings} }, $warning;

    if ( !( $self->quiet ) ) {
        print STDERR $warning;
    }
}

sub walk_filesystem {
    my $self = shift;

    my $filesystem = $self->WIP->wipmeta->get_filesystem(1);
    die "Error getting list of identifiers on filesystem\n" if ( !$filesystem );

    foreach my $fsid ( @{$filesystem} ) {
        my $docid = $fsid->{id};

        my ( $objid, $stage, $identifier ) = @{ $fsid->{key} };
        $self->onfs->{$stage}->{$objid}->{$identifier} = $docid;
        $self->aipfs->{$docid} = $fsid->{doc};
    }

    my @stages = keys %{ $self->WIP->stages };
    foreach my $stage (@stages) {
        my $path = $self->WIP->stages->{$stage};
        if ( $stage ne 'Trashcan' ) {
            $self->walk_stage( $stage, $path );
        }
    }
}

sub walk_stage {
    my ( $self, $name, $path ) = @_;

    $self->log->info("Scan stage $name at $path\n");

    if ( opendir( my $dh, $path ) ) {
        while ( readdir $dh ) {
            next if $_ eq "." || $_ eq "..";
            my $fullpath = $path . "/" . $_;
            if ( !-d $fullpath ) {
                warn "$_ is not a directory at $path\n";
                next;
            }
            if ( exists $self->configdocs->{$_} ) {
                $self->walk_identifiers( $name, $path, $_ );
            }
            else {
                if ( !rmdir($fullpath) ) {
                    warn "Can't remove invalid ConfigID at $fullpath: $!\n";
                }
            }
        }
        closedir $dh;

        foreach my $configid ( keys %{ $self->configdocs } ) {
            my $fullpath = $path . "/" . $configid;
            if ( !-d $fullpath ) {
                if ( !mkdir($fullpath) ) {
                    warn "Can't create ConfigID at $fullpath: $!\n";
                }
            }
        }
    }
    else {
        die "Couldn't open $path\n";
    }
}

sub hasSameClassify {
    my ( $nclassify, $oclassify ) = @_;

    my @nkeys = sort( keys %{$nclassify} );
    my @okeys = sort( keys %{$oclassify} );
    if ( scalar(@nkeys) != scalar(@okeys) ) {
        return 0;
    }
    for my $i ( 0 .. $#nkeys ) {
        return 0 if $nkeys[$i] ne $okeys[$i];
        return 0 if $nclassify->{ $nkeys[$i] } ne $oclassify->{ $okeys[$i] };
    }
    return 1;
}

sub walk_identifiers {
    my ( $self, $name, $path, $configid ) = @_;

    my $depositor = $self->WIP->config_depositor($configid);
    die "ConfigID `$configid` doesn't have a depositor\n" if ( !$depositor );

    $self->log->info(
        "Scan for identifiers for stage $name at $path/$configid\n");

    if ( opendir( my $dh, "$path/$configid" ) ) {
        while ( readdir $dh ) {
            next if $_ eq "." || $_ eq "..";
            next
              if $_ eq "dirlist.txt"
              || $_ eq "imagelist.txt"
              || $_ eq "ReelImageData.csv";
            my $identifier = $_;

            my $idpath = $path . "/" . $configid . "/" . $identifier;
            if ( -d $idpath ) {
                if ( index( $identifier, '.' ) != -1 ) {
                    warn
"Identifer '$identifier' at $name/$configid shouldn't have a period\n";
                }
                my $objid = $self->WIP->i2objid( $identifier, $configid );
                my $aip = "$depositor.$objid";

                if ( $self->WIP->objid_valid($objid) ) {
                    my $updatedoc = {
                        "filesystem" => encode_json(
                            {
                                stage      => $name,
                                configid   => $configid,
                                identifier => $identifier
                            }
                        )
                    };

                    # Short circuit database update if already set in database..
                    my $skipdb;

                    if (
                        exists $self->onfs->{$name}->{$configid}->{$identifier}
                      )
                    {
                        $skipdb =
                          $self->onfs->{$name}->{$configid}->{$identifier} eq
                          $aip;
                    }
                    if ( $name ne 'Trashcan' ) {
                        my $nclassify = $self->classify_obj_dir( $configid,
                            "$path/$configid/$identifier" );
                        $updatedoc->{'classify'} = encode_json($nclassify);

                        if ( exists $self->aipfs->{$aip} ) {
                            my $oclassify = $self->aipfs->{$aip}->{classify};
                            if ( !$oclassify ) {
                                $skipdb = 0;
                            }
                            if ($skipdb) {
                                $skipdb =
                                  hasSameClassify( $nclassify, $oclassify );
                            }
                        }
                        else {
                            $skipdb = 0;
                        }
                    }
                    if ( !$skipdb ) {
                        my $retdata =
                          $self->WIP->wipmeta->update_filesystem( $aip,
                            $updatedoc );
                        if ( !$retdata ) {
                            warn "update_filesystem didn't return data\n";
                        }
                        elsif (exists $retdata->{'stage'}
                            || exists $retdata->{'configid'}
                            || exists $retdata->{'identifier'} )
                        {

                            # Check if there is a duplicate
                            my @stages = keys %{ $self->WIP->stages };
                            foreach my $stage (@stages) {
                                if ( $stage ne $name ) {
                                    if (  -d $self->WIP->stages->{$stage} . "/"
                                        . $configid . "/"
                                        . $identifier )
                                    {
                                        warn(
"Duplicate: $configid/$identifier exists in both $name and $stage\n"
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
                else {
                    warn "OBJID=$objid in $name/$configid is invalid\n";
                }
            }
            else {
                warn
"Identifier $identifier not directory at: $name/$configid/$identifier\n";
            }
        }
        closedir $dh;
    }
    else {
        die "Couldn't open $path/$configid\n";
    }
}

sub upcount {
    my ( $classify, $classification ) = @_;

    if ( defined $classify->{$classification} ) {
        $classify->{$classification}++;
    }
    else {
        $classify->{$classification} = 1;
    }
}

sub classify_obj_dir {
    my ( $self, $configid, $path ) = @_;

    my $classify = {};

    if ( opendir( my $dh, $path ) ) {
        while ( readdir $dh ) {
            next if $_ eq "." || $_ eq "..";
            if ( -d "$path/$_" ) {
                upcount( $classify, "directory" );
                next;
            }
            my $fileconfig = $self->WIP->find_fileconfig( $configid, $_ );
            if ( defined $fileconfig ) {
                if ($fileconfig) {
                    next if ( $fileconfig->{ignore} );
                    my $class = $fileconfig->{class};
                    if ( $class && length($class) > 1 ) {
                        upcount( $classify, $class );
                    }
                    else {
                        upcount( $classify, "noclass" );
                    }
                }
                else {
                    upcount( $classify, "unknown" );
                }
            }
            else {
                upcount( $classify, "noclassify" );
            }
        }
        closedir $dh;
    }
    else {
        die "Couldn't open $path\n";
    }
    return $classify;
}

sub walk_dbfilesystem {
    my $self = shift;

    my $filesystem = $self->WIP->wipmeta->get_filesystem();
    die "Error getting list of identifiers on filesystem\n" if ( !$filesystem );

    my $stages = $self->WIP->stages;

    foreach my $fsid ( @{$filesystem} ) {
        my $docid = $fsid->{id};
        my ( $objid, $stage, $identifier ) = @{ $fsid->{key} };
        $stage = 'Trashcan'
          if ( !$stage )
          ; # Blank/undefined stage shouldn't be in database, so this will fix old entries.
        if ( -d $stages->{$stage} ) {
            if ( !-d $stages->{$stage} . "/" . $objid . "/" . $identifier ) {
                if ( $stage ne 'Trashcan' ) {
                    warn $stage . "/"
                      . $objid . "/"
                      . $identifier
                      . " no longer exists\n";
                }
                my $retdata = $self->WIP->wipmeta->update_filesystem(
                    $docid,
                    {
                        "filesystem" => encode_json(
                            {
                                stage => ''
                            }
                        )
                    }
                );
                if ( !$retdata ) {
                    warn "update_filesystem for $docid didn't return data\n";
                }
            }
        }
        else {
            warn "Stage $stage in $docid not valid\n";
        }
    }
}

sub gen_report {
    my $self   = shift;
    my $report = '';
    my $res;

    #
    # Get and add warnings to report
    #
    if ( scalar( @{ $self->{warnings} } ) ) {
        $report .= "The following warnings were raised:\n\n";

        $report .= " * " . join( " * ", @{ $self->{warnings} } );
    }

    #
    # Get the filesystem statistics
    #
    $res = $self->WIP->wipmeta->get(
        "/"
          . $self->WIP->wipmeta->database
          . "/_design/tdr/_view/filesystem?group_level=2",
        {},
        { deserializer => 'application/json' }
    );
    if ( !$res->data && $res->code != 200 ) {
        die "_view/filesystem GET return code: "
          . $res->code . " ("
          . $res->response->content . ")\n";
    }
    if ( scalar( @{ $res->data->{rows} } ) ) {
        $report .=
          "\n\nPackaging scan found in the following stage/configurations:\n\n";

        foreach my $row ( @{ $res->data->{rows} } ) {
            next
              if ( !( $row->{key}[1] ) || $row->{key}[1] eq '' )
              ;    # skip if stage='' for deleted files
            my $count = $row->{value};
            my $wipdir = join( "/", reverse @{ $row->{key} } );

            $report .= " * $wipdir ($count)\n";
        }
    }

    #
    # Get Disk Free
    #
    $report .= "\n\nPackaging disk usage:\n\n";

    my $wippath = $self->WIP->wipconfig->{wipdir};
    my $ref     = dfportable($wippath);

    my $totalh = formatSize( $ref->{blocks} );
    $report .= "Total: $totalh ($ref->{blocks} bytes)\n";

    my $freeh = formatSize( $ref->{bfree} );
    $report .= "Free: $freeh ($ref->{bfree} bytes)\n";

    my $usedh = formatSize( $ref->{bused} );
    $report .= "Used: $usedh ($ref->{bused} bytes) = $ref->{per}%\n\n";

    #
    # Get the ingest statistics
    #
    $res = $self->WIP->wipmeta->get(
        "/"
          . $self->WIP->wipmeta->database
          . "/_design/tdr/_view/manifestdate?group_level=3&limit=10&descending=true",
        {},
        { deserializer => 'application/json' }
    );
    if ( !$res->data && $res->code != 200 ) {
        die "_view/filesystem GET return code: "
          . $res->code . " ("
          . $res->response->content . ")\n";
    }

    if ( scalar( @{ $res->data->{rows} } ) ) {
        $report .=
          "\n\nCount of AIPs with the 10 most recent ingest dates:\n\n";

        foreach my $row ( @{ $res->data->{rows} } ) {
            my $count = $row->{value};
            my $day = join( "-", @{ $row->{key} } );

            $report .= " * $day ($count)\n";
        }
    }

    #
    # Get the repository file size statistics
    #
    if ( $self->WIP->tdrepo ) {
        $res = $self->WIP->tdrepo->get(
            "/"
              . $self->WIP->tdrepo->database
              . "/_design/tdr/_view/repofilesize?group_level=1",
            {},
            { deserializer => 'application/json' }
        );
        if ( !$res->data && $res->code != 200 ) {
            die "_view/repofilesize GET return code: "
              . $res->code . " ("
              . $res->response->content . ")\n";
        }

        if ( scalar( @{ $res->data->{rows} } ) ) {
            $report .= "\n\nRepository file size:\n\n";

            foreach my $row ( @{ $res->data->{rows} } ) {
                my $size       = $row->{value}->{sum};
                my $count      = $row->{value}->{count};
                my $repository = $row->{key}[0];
                my $sizehr     = formatSize($size);

                $report .=
" * '$repository' has $count AIPs using $sizehr ($size bytes) of storage\n";
            }
        }
    }

    # Email or output to screen the report
    if ( index( $self->report, "@" ) == -1 ) {
        print $report;
    }
    else {
        my $message = Email::MIME->create(
            header_str => [
                From =>
                  '"Canadiana OAIS Packaging Reporter" <noreply@canadiana.ca>',
                To      => $self->report,
                Subject => 'Canadiana OAIS Packaging report',
            ],
            attributes => {
                encoding => 'quoted-printable',
                charset  => 'UTF-8',
            },
            body_str => $report,
        );
        sendmail($message);
    }
}

# https://kba49.wordpress.com/2013/02/17/format-file-sizes-human-readable-in-perl/
sub formatSize {
    my $size = shift;
    my $exp  = 0;

    state $units = [qw(B KB MB GB TB PB)];

    for (@$units) {
        last if $size < 1024;
        $size /= 1024;
        $exp++;
    }

    return
      wantarray
      ? ( $size, $units->[$exp] )
      : sprintf( "%.2f %s", $size, $units->[$exp] );
}

1;
