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
/* global CKEDITOR */
const {Component, createElement: ce} = require('react'),
    PropTypes = require('prop-types');

const TinyMCE = require('react-tinymce');

const {default: {createEntity}} = require('../api');

class CKEditorWrapper extends Component {

    componentDidMount() {
        this._editor = CKEDITOR.replace(this._container);
    }

    componentWillUnmount() {
        this._editor.destroy();
    }

    getValue() {
        return this._editor.getData();
    }

    render() {
        return ce('textarea', {defaultValue: this.props.formData,
                               ref: n => this._container = n});
    }
}
CKEditorWrapper.propTypes = {
    formData: PropTypes.object,
};


function fileInputHandler(file, cb) {
    var reader = new FileReader();
    reader.onload = function(e) {
        createEntity('file', {data: e.target.result, title: file.name})
            .then(doc => {
                if (doc.errors && doc.errors.length) {
                    alert(JSON.stringify(doc.errors, null, 2));
                    return;
                }
                cb(doc.download_url, doc.title);
            });
    };
    reader.readAsDataURL(file);
}


class TinyMCEWrapper extends Component {

    shouldComponentUpdate(nextProps) {
        // if value hasn't changed between the onChange event and now,
        // don't re-render the component or we'll loose the focus
        return nextProps.value !== this.value;
    }

    filePickerHandler(callback, value, meta) {
        const input = document.getElementById('tinymcefile');
        if (meta.filetype === 'image') {
            // Provide image and alt text for the image dialog
            input.click();
            input.onchange = () => fileInputHandler(
                input.files[0], (url, title) => callback(url, {alt: title}));
        } else if (meta.filetype === 'file') {
            input.click();
            input.onchange = () => fileInputHandler(
                input.files[0], (url, title) => callback(url,
                                                         {text: title, title}));
        }
    }

    render() {
        this.value = this.props.value;
        return ce(TinyMCE, {
            content: this.props.value,
            config: {
                menubar: false,
                plugins: 'link image code lists media',
                file_picker_types: 'image file',
                file_picker_callback: this.filePickerHandler,
                toolbar: 'undo redo | formatselect | bold italic | blockquote | bullist numlist | alignleft aligncenter alignright | link | image | media | code',
                height: 400,
            },
            onChange: event => {
                // store value before triggering onChange to be able to
                // test new props against current value in shouldComponentupdate
                this.value = event.target.getContent();
                this.props.onChange(this.value)
            },
        });
    }
}
TinyMCEWrapper.propTypes = {
    value: PropTypes.string,
    onChange: PropTypes.func.isRequired,
};

module.exports = {
    CKEditorWrapper,
    TinyMCEWrapper,
};
