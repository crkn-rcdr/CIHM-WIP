package CIHM::WIP::REST::tdrepo;

use strict;
use Carp;
use DateTime;
use JSON;

use Moo;
with 'Role::REST::Client';
use Types::Standard qw(HashRef Str Int Enum HasMethods);

=head1 NAME

CIHM::WIP::REST::tdrepo - Subclass of Role::REST::Client used to
interact with "tdrepo" CouchDB database

=head1 SYNOPSIS

    my $t_repo = CIHM::WIP::REST::tdrepo->new($args);
      where $args is a hash of arguments.  In addition to arguments
      processed by Role::REST::Client we have the following 

      $args->{conf} is as defined in CIHM::TDR::TDRConfig
      $args->{database} is the Couch database name.

=cut


sub BUILD {
    my $self = shift;
    my $args = shift;

    $self->{LocalTZ} = DateTime::TimeZone->new( name => 'local' );
    $self->{conf} = $args->{conf}; 
    $self->{database} = $args->{database};
}

# Simple accessors for now -- Do I want to Moo?
sub database {
    my $self = shift;
    return $self->{database};
}

=head2 get_newestaip($params)

Parameter is a hash of possible parameters
  keys - An array of keys to look up
=cut
sub get_newestaip {
    my ($self, $params) = @_;
    my ($res, $code);
    my $restq = {};

    if ($params->{keys}) {
        $restq->{keys}=$params->{keys};
    }
    $self->type("application/json");
    $res = $self->post("/".$self->{database}."/_design/tdr/_view/newestaip?group=true",$restq, {deserializer => 'application/json'});
    if ($res->code == 200) {
        if (defined $res->data->{rows}) {
            return $res->data->{rows};
        } else {
            return [];
        }
    }
    else {
        warn "_view/newestaip GET return code: ".$res->code."\n"; 
        return;
    }
}

1;
