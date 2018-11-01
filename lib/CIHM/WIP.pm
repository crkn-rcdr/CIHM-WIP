package CIHM::WIP;
use strict;
use warnings;

use Config::General;
use XML::LibXML;
use CIHM::WIP::REST::tdrepo;
use CIHM::WIP::REST::wipmeta;
use Data::Dumper;

=head1 NAME

CIHM::WIP - The great new CIHM::WIP!

=head1 VERSION

Version 0.18

=cut

our $VERSION = '0.18';


sub new {
    
    my($self, $configpath) = @_;
    my $wip = {};

    if ($configpath) {
        my $config = new Config::General(
            -ConfigFile => $configpath
            );
        $wip->{wipconfig} = {$config->getall()};
    }

    # Undefined if no <tdrepo> config block
    if (exists $wip->{wipconfig}->{tdrepo}) {
        $wip->{tdrepo} = new CIHM::WIP::REST::tdrepo (
            server =>  $wip->{wipconfig}->{tdrepo}{server},
            database =>  $wip->{wipconfig}->{tdrepo}{database},
            type   => 'application/json',
            conf   => $configpath,
            clientattrs => {timeout => 3600},
            );
    }

    # Undefined if no <wipmeta> config block
    if (exists $wip->{wipconfig}->{wipmeta}) {
        $wip->{wipmeta} = new CIHM::WIP::REST::wipmeta (
            server =>  $wip->{wipconfig}->{wipmeta}{server},
            database =>  $wip->{wipconfig}->{wipmeta}{database},
            type   => 'application/json',
            conf   => $configpath,
            clientattrs => {timeout => 3600},
            );
    }

    # Any <depositor> blocks
    $wip->{depositors} = $wip->{'wipconfig'}->{'depositor'};

    return bless($wip);
}

sub wipconfig {
    my ($self) = shift;
    return $self->{wipconfig};
}
sub tdrepo {
    my ($self) = shift;
    return $self->{tdrepo};
}
sub wipmeta {
    my ($self) = shift;
    return $self->{wipmeta};
}

sub stages {
    my ($self) = shift;

    if (exists $self->wipconfig->{'stages'} &&
        ref($self->wipconfig->{'stages'}) eq "HASH") {
        return $self->wipconfig->{'stages'};
    }
    return;
}
sub findstagei {
    my ($self) = shift;
    my ($findstage) = lc shift;
    my $stages=$self->stages;
    if ($stages) {
        foreach my $thisstage (keys $stages) {
            if (lc($thisstage) eq $findstage) {
                return $thisstage;
            }
        }
    }
}


sub configdocs {
    my ($self) = shift;

    if ($self->wipmeta) {
        if (!$self->{configdocs}) {
            $self->{configdocs}=$self->wipmeta->get_configdocs;
        }
        return $self->{configdocs};
    }
}
sub configid_valid {
    my ($self,$configid) = @_;
    return (exists $self->configdocs->{$configid});

}

# Similar to functions in CIHM::TDR::Repository
# Defined by http://www.canadiana.ca/schema/2012/mets/csip.xml
sub depositor_valid {
    my ($self,$depositor) = @_;

    if ($self->{depositors}) {
        # If we have a list of valid depositors, use that
        return exists $self->{'depositors'}->{$depositor};
    } else {
        # Otherwise, just check if it matches the pattern of characters
        return $depositor =~ /^[a-z]+$/;
    }
}
sub objid_valid {
    my ($self,$objid) = @_;
    return $objid =~ /^[A-Za-z0-9_]{5,64}$/;
}


# Takes a UID (or a full path that ends in a UID), and converts to
# an array (depositor,OBJID,AIPid)
sub parse_uid {
    my($self, $uid) = @_;

    # Pattern based on CIHM::TDR::Repository->aip_valid()
    if ($uid =~ /[\/]*([a-z]+)\.([A-Za-z0-9_]{5,64})[\/]*$/) {
        my $depositor = $1;
        my $objid = $2;
        my @aip =($depositor,$objid,"$depositor.$objid");
        return(@aip);
    }
}

# Find the rsync URL for a given path (should be result of Cwd->realpath() )
# returns an array:
#   First element is rsyncurl
#   Second element is rest of pathname with prefix removed
sub find_rsync {
    my($self, $path) = @_;
    if (defined $self->wipconfig->{paths}) {
        foreach my $checkpath (keys $self->wipconfig->{'paths'}) {
            my $rsyncurl =  $self->wipconfig->{'paths'}->{$checkpath};
            if (substr($path,0,length($checkpath)) eq $checkpath) {
                return ($rsyncurl,substr($path,length($checkpath)));
            }
        }
    }
}

sub parse_mets {
    my ($self,$metadata) = @_;

    my $xml = XML::LibXML->new->parse_file($metadata);
    my $xml_xc = XML::LibXML::XPathContext->new($xml);
    return $xml_xc->findvalue("/mets:mets/\@OBJID");
}

sub check_aip_exists {
    my ($self,$uid) = @_;

    if ($self->tdrepo) {
        my $newestaip=$self->tdrepo->get_newestaip({keys => [ $uid ]});
        if ($newestaip && defined $newestaip->[0]) {
            return 1;
        }
    }
}

sub find_fileconfig {
    my ($self,$configid,$filename)= @_;

    if (exists $self->configdocs->{$configid} && 
        exists $self->configdocs->{$configid}->{'fileconfig'}) {
        my $fileconfigs = $self->configdocs->{$configid}->{'fileconfig'};
        if (ref $fileconfigs eq 'ARRAY') {
            foreach my $fileconfig (@$fileconfigs) {
                my $regex = $fileconfig->{'regex'};
                if ($filename =~ $regex) {
                    return $fileconfig;
                }
            }
            # Not found, return a value that is false
            return 0;
        } else {
            warn "fileconfig for '$configid' isn't an array\n";
        }
    }
    # No config or bad config, return without value
    return;
}



sub i2objid {
    my ($self,$identifier,$configid) = @_;

    if (exists $self->configdocs->{$configid} && 
        exists $self->configdocs->{$configid}->{'i2objid'}) {
        my $filters = $self->configdocs->{$configid}->{'i2objid'};
        if (ref $filters eq 'ARRAY') {
            foreach my $filter (@$filters) {
                my $search = $filter->{'search'};
                my $replace = $filter->{'replace'};
 
                # Why not /ee discussed here http://stackoverflow.com/questions/392643/how-to-use-a-variable-in-the-replacement-side-of-the-perl-substitution-operator

                # Does only one substitution at a time, so loop until done.
                my $previous='';
                while ($identifier ne $previous) {
                    $previous = $identifier;
                    # Capture first 
                    my @items = ( $identifier =~ $search );
                    $identifier =~ s/$search/$replace/; 
                    for( reverse 0 .. $#items ){ 
                        my $n = $_ + 1; 
                        #  Many More Rules can go here, ie: \g matchers  and \{ } 
                        $identifier =~ s/\\$n/${items[$_]}/g ;
                        $identifier =~ s/\$$n/${items[$_]}/g ;
                    }
                }
            }
        } else {
            warn "i2objid for '$configid' isn't an array\n";
        }
    }
    return $identifier;
}

sub config_depositor {
    my ($self,$configid) = @_;

    if (exists $self->configdocs->{$configid} && 
        exists  $self->configdocs->{$configid}->{'depositor'}) {
        return $self->configdocs->{$configid}->{'depositor'};
    }
}

1;
