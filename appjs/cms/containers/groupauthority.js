/*
 * Copyright © LOGILAB S.A. (Paris, FRANCE) 2016-2019
 * Contact http://www.logilab.fr -- mailto:contact@logilab.fr
 *
 * This software is governed by the CeCILL-C license under French law and
 * abiding by the rules of distribution of free software. You can use,
 * modify and/ or redistribute the software under the terms of the CeCILL-C
 * license as circulated by CEA, CNRS and INRIA at the following URL
 * "http://www.cecill.info".
 *
 * As a counterpart to the access to the source code and rights to copy,
 * modify and redistribute granted by the license, users are provided only
 * with a limited warranty and the software's author, the holder of the
 * economic rights, and the successive licensors have only limited liability.
 *
 * In this respect, the user's attention is drawn to the risks associated
 * with loading, using, modifying and/or developing or reproducing the
 * software by the user in light of its specific status of free software,
 * that may mean that it is complicated to manipulate, and that also
 * therefore means that it is reserved for developers and experienced
 * professionals having in-depth computer knowledge. Users are therefore
 * encouraged to load and test the software's suitability as regards their
 * requirements in conditions enabling the security of their systemsand/or
 * data to be ensured and, more generally, to use and operate it in the
 * same conditions as regards security.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL-C license and that you accept its terms.
 */

import React from 'react';
import {connect} from 'react-redux';
import PropTypes from 'prop-types';
import {throttle} from 'lodash/function';

import Api from '../api';


function Spinner() {
    return <i className="fa fa-spin fa-spinner"></i>;
}


class IdEntry extends React.Component {
    constructor(props) {
        super(props);
        this.state = {validating: false, success: null, msg: null};
        this.onChange = this.onChange.bind(this);
        this.checkNewId = throttle(this.checkNewId.bind(this), 300);
    }

    checkNewId(value) {
        const {authtype, authtitle} = this.props;
        this.setState({validating: true, success: null, msg: null});
        fetch(Api.buildUrl(`/${authtype}/${value}`),
            {credentials: 'same-origin', headers: {Accept: 'application/json'}})
            .then(res => {
                if (res.status === 404) {
                    this.setState({msg: "cet identifiant n'existe pas en base"})
                    throw new Error('not found');
                }
                if(res.headers.get("content-type") &&
                   res.headers.get("content-type").toLowerCase().indexOf("application/json") >= 0) {
                    return res.json()
                }
            })
            .then(data => {
                this.setState({
                    validating: false,
                    success: true,
                    msg: `l'autorité "${data.dc_title}" sera fusionnée avec "${authtitle}"`,
                });
            })
            .catch(err => {
                this.setState({validating: false, success: false});
                console.error(err);
            })
    }

    onChange(ev) {
        const {value} = ev.target,
            {onChange, idx} = this.props;
        this.checkNewId(value);
        onChange(idx, value);
    }

    render() {
        const {value} = this.props,
            {validating, success, msg} = this.state;
        let icon, classname = 'form-group has-feedback', label = null;
        if (validating) {
            icon = <Spinner />;
        } else if (success === true) {
            icon = <i className="fa fa-check"></i>;
            classname += ' has-success';
        } else if (success === false) {
            icon = <i className="fa fa-times"></i>;
            classname += ' has-error';
        } else {
            icon = null;
        }
        if (msg !== null) {
            label = <label className="control-label">{msg}</label>;
        }
        return (
            <div className={classname}>
                {label}
                <input type="text" className="form-control" value={value} onChange={this.onChange} />
                <span className="form-control-feedback">{icon}</span>
            </div>
        );
    }
}
IdEntry.propTypes = {
    value: PropTypes.string.isRequired,
    onChange: PropTypes.func.isRequired,
    authtype: PropTypes.string.isRequired,
    authtitle: PropTypes.string.isRequired,
    idx: PropTypes.number.isRequired,
};


function IdList({authtype, authtitle, ids, onIdChange}) {
    return (
        <ul className="list-group">
            {ids.map((id, idx) => (
                <li className="list-group-item" key={idx}>
                    <IdEntry authtype={authtype} authtitle={authtitle} idx={idx} value={id} onChange={onIdChange} />
                </li>
             ))}
        </ul>

    );
}
IdList.propTypes = {
    ids: PropTypes.array.isRequired,
    onIdChange: PropTypes.func.isRequired,
    authtype: PropTypes.string.isRequired,
    authtitle: PropTypes.string.isRequired,
};


class GroupAuthorityComp extends React.Component {
    constructor(props) {
        super(props);
        this.state = {ids: [], submitting: false};
        this.onAdd = this.onAdd.bind(this);
        this.onIdChange = this.onIdChange.bind(this);
        this.onSubmit = this.onSubmit.bind(this);
    }

    onSubmit(ev) {
        ev.preventDefault();
        const {entity} = this.props,
            {ids} = this.state,
            eid = entity.get('eid');
        this.setState({submitting: true});
        Api.jsonFetch(`/fa/authority/${eid}/_group`, {method: 'POST', body: JSON.stringify(ids)})
            .then(() => document.location.reload())
            .catch(err => console.error(err));

    }

    onIdChange(idx, newvalue) {
        const {ids} = this.state;
        ids[idx] = newvalue;
        this.setState({ids});
    }

    onAdd() {
        const {ids} = this.state;
        ids.push('');
        this.setState({ids});
    }

    render() {
        const {entity} = this.props,
            {ids, submitting} = this.state,
            title = entity.get('dc_title'),
            i18netype = entity.get('i18n_cw_etype'),
            etype = entity.get('cw_etype');
        return (
            <div>
                <h1>
                 {i18netype} : &quot;{title}&quot;
                </h1>
                <div className="cms_add_link">
                Ajouter des identifiants des autorités qui seront fusionnées dans &quot;{title}&quot;
                <button className="btn" onClick={this.onAdd}>+</button>
               </div>
                <form onSubmit={this.onSubmit}>
                    <IdList authtype={etype} authtitle={title} ids={ids} onIdChange={this.onIdChange} />
                    <button className="btn btn-default">annuler</button>
                    <button disabled={!ids.length}className="btn btn-primary" type="submit">
                        {submitting ? <Spinner /> : null}
                        enregistrer
                    </button>
                </form>
            </div>
        )
    }
}
GroupAuthorityComp.propTypes = {
    entity: PropTypes.object.isRequired,
};

export const GroupAuthority = connect(
    function mapStateToProps(state) {
        return {entity: state.getIn(['model', 'entity'])};
    },
)(GroupAuthorityComp);
