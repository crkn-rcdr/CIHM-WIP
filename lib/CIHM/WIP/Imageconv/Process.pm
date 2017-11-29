package CIHM::WIP::Imageconv::Process;

use 5.014;
use strict;
use Try::Tiny;
use File::Spec;
use File::Path qw(make_path remove_tree);
use File::Copy;
use JSON;
use Switch;
use POSIX qw(strftime);
use Image::Magick;
use Data::Dumper;

=head1 NAME

CIHM::WIP::Imageconv::Process - Handles conversion of images in WIP directories.

=head1 SYNOPSIS

    my $t_repo = CIHM::WIP::Imageconv::Process->new($args);
      where $args is a hash of arguments.

      $args->{configpath} is as defined in CIHM::WIP

=cut


sub new {
    my($class, $args) = @_;
    my $self = bless {}, $class;

    if (ref($args) ne "HASH") {
        die "Argument to CIHM::WIP::Imageconv::Process->new() not a hash\n";
    };
    $self->{args} = $args;

    if (!$self->aip) {
        die "Parameter 'aip' is mandatory\n";
    }
    if (!$self->WIP) {
        die "CIHM::WIP instance parameter is mandatory\n";
    }
    if (!$self->log) {
        die "log object parameter is mandatory\n";
    }
    if (!$self->hostname) {
        die "hostname parameter is mandatory\n";
    }
    $self->{aipdata}=$self->wipmeta->get_aip($self->aip);
    if (!$self->aipdata) {
        die "Failed retrieving AIP data\n";
    }
    return $self;
}

# Simple accessors for now -- Do I want to Moo?
sub args {
    my $self = shift;
    return $self->{args};
}
sub aip {
    my $self = shift;
    return $self->args->{aip};
}
sub aipdata {
    my $self = shift;
    return $self->{aipdata};
}
sub hostname {
    my $self = shift;
    return $self->args->{hostname};
}
sub log {
    my $self = shift;
    return $self->args->{log};
}
sub WIP {
    my $self = shift;
    return $self->args->{WIP};
}
sub wipmeta {
    my $self = shift;
    return $self->WIP->{wipmeta};
}
sub configdocs {
    my $self = shift;
    return $self->WIP->configdocs;
}
sub configid {
    my $self = shift;
    return $self->{configid};
}
sub myconfig {
    my $self = shift;
    return $self->{myconfig};
}
sub workdir {
    my $self = shift;
    return $self->{workdir};
}
sub job {
    my $self = shift;
    return $self->{job};
}

sub process {
    my ($self) = @_;

    $self->{job} = $self->aipdata->{'processReq'}[0];

    $self->log->info($self->aip.": Accepted job. processReq = ". encode_json($self->job));

    # Set more per-AIP information
    if (!defined $self->aipdata->{filesystem}) {
        die "Required filesystem field not defined\n";
    }

    $self->{configid} = $self->aipdata->{filesystem}->{configid} or
        die "Filesystem sub-field 'configid' not defined\n";
    $self->{myconfig} =$self->WIP->configdocs->{$self->configid} or
        die $self->configid." is not a valid configuration id\n";

    my ($stage,$stagedir,$identifier);

    $stage = $self->aipdata->{filesystem}->{stage} or
        die "Filesystem sub-field 'stage' not defined\n";
    $stagedir = $self->WIP->stages->{$stage} or
        die "Filesystem stage=$stage not configured\n";

    $identifier = $self->aipdata->{filesystem}->{identifier} or
        die "Filesystem sub-field 'identifier' not defined\n";

    $self->{workdir}=$stagedir."/".$self->configid."/".$identifier;

    if (! -d $self->workdir) {
        die "Working directory ".$self->workdir." doesn't exist\n";
    }

    $self->log->info($self->aip.": Workdir=".$self->workdir);


    if($self->job->{fileconfig}) {
        my @convertlist;

        if (opendir (my $dh, $self->workdir)) {
            while(readdir $dh) {
                next if $_ eq "." || $_ eq "..";
                if (-d $self->workdir."/$_") {
                    next;
                }
                my $fileconfig=$self->WIP->find_fileconfig($self->configid,$_);
                if ($fileconfig) {
                    next if ($fileconfig->{ignore});
                    my $magick=$fileconfig->{magick};
                    my $ext=$fileconfig->{ext};
                    next if (!$magick || !$ext);
                    push @convertlist, { file => $_, fileconfig => $fileconfig};
                }
            }
            closedir $dh;
        } else {
            die "Couldn't open ".$self->workdir.": $!\n";
        }

        $self->log->info($self->aip.": ".scalar(@convertlist)." images need conversion.");

        if (@convertlist) {
            my $imageconv=$self->workdir."/imageconv";
            if (! -d $imageconv) {
                mkdir $imageconv or die "Can't make $imageconv : $!";
            }
            my $imageorig=$imageconv."/orig";
            if (! -d $imageorig) {
                mkdir $imageorig or die "Can't make $imageorig : $!";
            }

            my @successlist;
            foreach my $conv (@convertlist) {
                my $file=$conv->{file};
                my $fileconfig=$conv->{fileconfig};
                my $ext = $fileconfig->{ext};

                my $sourcepath=$self->workdir."/".$file;
                my $origpath=$imageorig."/".$file;

                my $convfile = $file;
                $convfile =~ s/\.[^.]*$//;
                $convfile .= ".".$ext;
                my $convpath=$imageconv."/".$convfile;
                my $destpath=$self->workdir."/".$convfile;

                if (-e $origpath) {
                    unlink $origpath or die "Can't remove $origpath: $!\n";
                }
                if (!link($sourcepath,$origpath)) {
                    die "Error creating link from $sourcepath to $origpath: $!\n";
                }
                if (-e $convpath) {
                    unlink $convpath or die "Can't remove $convpath: $!\n";
                }
                $self->domagick($origpath,$convpath,$fileconfig->{magick});
                push @successlist, { 
                    unlink => $sourcepath, 
                    sourcelink => $convpath,
                    destlink => $destpath
                };
            }

            # Only if we got this far do we move the converted files into place
            foreach my $conv (@successlist) {
                unlink($conv->{unlink}) 
                    or die "Can't remove ".$conv->{unlink}.": $!\nObject directory may need cleanup!";
                if (!link($conv->{sourcelink},$conv->{destlink})) { 
                    die "Can't link ".$conv->{sourcelink}.
                        " to ".$conv->{destlink}.": $!\nObject directory may need cleanup!";
                }
            }
        }
    }

    $self->log->info($self->aip.": Completed job.");
    return;
}


sub domagick {
    my ($self,$origpath,$convpath,$magick) = @_;

    my $p = new Image::Magick;

    my $status = $p->Read($origpath);

    if ($status) {
        switch ($status) {
            # Skip Exif ImageUniqueID
            case /Unknown field with tag 42016 / {}
            else {
                die "$origpath Read: $status\n";
            }
        }
    }

    foreach my $t (@{$magick}) {
        if (defined $t->{set}) {
            $status = $p->Set(%{$t->{set}});
            die "$origpath Set: $status\n" if "$status";
        }
    }
    $status = $p->Write($convpath);
    die "$convpath Write: $status\n" if "$status";
}

1;
